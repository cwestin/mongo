/**
 * Copyright (c) 2012 10gen Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "pch.h"
#include "mongo/db/commands/pipeline_d.h"

#include "mongo/db/cursor.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/commands/pipeline.h"
#include "mongo/db/pipeline/dependency_tracker.h"
#include "mongo/db/pipeline/document_source.h"


namespace mongo {

    intrusive_ptr<DocumentSourceCursor> PipelineD::prepareCursorSource(
        const intrusive_ptr<Pipeline> &pPipeline,
        const string &dbName,
        const intrusive_ptr<ExpressionContext> &pExpCtx) {

        Pipeline::SourceVector *pSources = &pPipeline->sourceVector;

        /*
          Analyze dependency information.

          At this point, all external static optimizations should have been
          done. The only other changes that could happen to the pipeline after
          this are to remove initial $match or $sort items because they will
          be handled by the underlying query. But we need to analyze the
          dependencies before that so that references in those are included
          in the dependency analysis.

          This checks dependencies from the end of the pipeline back to the
          front of it, and finally passes that to the input source before we
          execute the pipeline.
        */
        intrusive_ptr<DependencyTracker> pTracker(DependencyTracker::create());
        for(Pipeline::SourceVector::reverse_iterator iter(pSources->rbegin()),
                listBeg(pSources->rend()); iter != listBeg; ++iter) {
            intrusive_ptr<DocumentSource> pTemp(*iter);
            pTemp->manageDependencies(pTracker);
        }

        DEV {
            /* list the dependencies in the log for development */
            stringstream ss;
            pTracker->listDependencies(ss);
            (log() << ss.str()).flush();
        }

        /* look for an initial match */
        BSONObjBuilder queryBuilder;
        bool initQuery = pPipeline->getInitialQuery(&queryBuilder);
        if (initQuery) {
            /*
              This will get built in to the Cursor we'll create, so
              remove the match from the pipeline
            */
            pSources->erase(pSources->begin());
        }

        /*
          Create a query object.

          This works whether we got an initial query above or not; if not,
          it results in a "{}" query, which will be what we want in that case.

          We create a pointer to a shared object instead of a local
          object so that we can preserve it for the Cursor we're going to
          create below.  See DocumentSourceCursor::addBsonDependency().
         */
        shared_ptr<BSONObj> pQueryObj(new BSONObj(queryBuilder.obj()));

        /*
          If the result set is closed, we can limit the fields we fetch.
          There are two parts to that:
          (1) Try to do an index-only query. To do this, we supply a
          ParsedQuery with the select-list when we create the cursor.
          TODO This also requires the flag from SERVER-6023 in order not to
          gum up a sort
          (2) Supply DocumentSourceCursor with the list so that it only
          passes along the required fields whether (1) happens or not.

          Either way, we need to build a select-list.
        */
        BSONObjBuilder selectBuilder;
        if (pTracker->isClosedSet())
            pTracker->buildSelectList(&selectBuilder);
        shared_ptr<BSONObj> pSelectObj(new BSONObj(selectBuilder.obj()));

        /*
          In order to send the select-list into the Cursor factory below,
          we need a ParsedQuery.

          LATER: if the query engine survives in its current form, early
          limits and skips may also be optimized here. Ideally, we get
          redo the query engine to be based on DocumentSources, and this all
          gets a lot simpler.
         */
        string fullName(dbName + "." + pPipeline->getCollectionName());
        shared_ptr<ParsedQuery> pParsedQuery(
            new ParsedQuery(fullName.c_str(), 0, 0, 0,
                            *pQueryObj, *pSelectObj));
        // TODO use this in the getCursor() calls below SERVER-5090

        /*
          Look for an initial sort; we'll try to add this to the
          Cursor we create.  If we're successful in doing that (further down),
          we'll remove the $sort from the pipeline, because the documents
          will already come sorted in the specified order as a result of the
          index scan.
        */
        const DocumentSourceSort *pSort = NULL;
        BSONObjBuilder sortBuilder;
        if (pSources->size()) {
            const intrusive_ptr<DocumentSource> &pSC = pSources->front();
            pSort = dynamic_cast<DocumentSourceSort *>(pSC.get());

            if (pSort) {
                /* build the sort key */
                pSort->sortKeyToBson(&sortBuilder, false);
            }
        }

        /* Create the sort object; see comments on the query object above */
        shared_ptr<BSONObj> pSortObj(new BSONObj(sortBuilder.obj()));

        /* for debugging purposes, show what the query and sort are */
        DEV {
            (log() << "\n---- query BSON\n" <<
             pQueryObj->jsonString(Strict, 1) << "\n----\n").flush();
            (log() << "\n---- sort BSON\n" <<
             pSortObj->jsonString(Strict, 1) << "\n----\n").flush();
            (log() << "\n---- fullName\n" <<
             fullName << "\n----\n").flush();
        }
        
        /*
          Create the cursor.

          If we try to create a cursor that includes both the match and the
          sort, and the two are incompatible wrt the available indexes, then
          we don't get a cursor back.

          So we try to use both first.  If that fails, try again, without the
          sort.

          If we don't have a sort, jump straight to just creating a cursor
          without the sort.

          If we are able to incorporate the sort into the cursor, remove it
          from the head of the pipeline.

          LATER - we should be able to find this out before we create the
          cursor.  Either way, we can then apply other optimizations there
          are tickets for, such as SERVER-4507.
         */
        shared_ptr<Cursor> pCursor;
        bool initSort = false;
        if (pSort) {
            /* try to create the cursor with the query and the sort */
            shared_ptr<Cursor> pSortedCursor(
                pCursor = NamespaceDetailsTransient::getCursor(
                    fullName.c_str(), *pQueryObj, *pSortObj));

            if (pSortedCursor.get()) {
                /* success:  remove the sort from the pipeline */
                pSources->erase(pSources->begin());

                pCursor = pSortedCursor;
                initSort = true;
            }
        }

        if (!pCursor.get()) {
            /* try to create the cursor without the sort */
            shared_ptr<Cursor> pUnsortedCursor(
                pCursor = NamespaceDetailsTransient::getCursor(
                    fullName.c_str(), *pQueryObj));

            pCursor = pUnsortedCursor;
        }

        /* wrap the cursor with a DocumentSource and return that */
        intrusive_ptr<DocumentSourceCursor> pSource(
            DocumentSourceCursor::create(pCursor, dbName, pExpCtx));

        pSource->setNamespace(fullName);

        /*
          Note the query, select, and sort

          This records them for explain, and keeps them alive; they are
          referenced (by reference) by the cursor, which doesn't make its
          own copies of them.
        */
        pSource->setQuery(pQueryObj);
        pSource->setSelect(pSelectObj);
        if (initSort)
            pSource->setSort(pSortObj);

        shared_ptr<void> pVoid(
            static_pointer_cast<void, ParsedQuery>(pParsedQuery));
        pSource->keepAlive(pVoid);

        return pSource;
    }

} // namespace mongo
