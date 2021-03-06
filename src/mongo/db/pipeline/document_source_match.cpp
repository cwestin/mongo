/**
*    Copyright (C) 2011 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "pch.h"

#include "db/pipeline/document_source.h"

#include "db/jsobj.h"
#include "db/matcher.h"
#include "db/pipeline/document.h"
#include "db/pipeline/expression.h"

namespace mongo {

    const char DocumentSourceMatch::matchName[] = "$match";

    DocumentSourceMatch::~DocumentSourceMatch() {
    }

    const char *DocumentSourceMatch::getSourceName() const {
        return matchName;
    }

    void DocumentSourceMatch::sourceToBson(
        BSONObjBuilder *pBuilder, bool explain) const {
        const BSONObj *pQuery = matcher.getQuery();
        pBuilder->append(matchName, *pQuery);
    }

    bool DocumentSourceMatch::accept(
        const intrusive_ptr<Document> &pDocument) const {

        /*
          The matcher only takes BSON documents, so we have to make one.

          LATER
          We could optimize this by making a document with only the
          fields referenced by the Matcher.  We could do this by looking inside
          the Matcher's BSON before it is created, and recording those.  The
          easiest implementation might be to hold onto an ExpressionDocument
          in here, and give that pDocument to create the created subset of
          fields, and then convert that instead.
        */
        BSONObjBuilder objBuilder;
        pDocument->toBson(&objBuilder);
        BSONObj obj(objBuilder.done());

        return matcher.matches(obj);
    }

    intrusive_ptr<DocumentSource> DocumentSourceMatch::createFromBson(
        BSONElement *pBsonElement,
        const intrusive_ptr<ExpressionContext> &pExpCtx) {
        uassert(15959, "the match filter must be an expression in an object",
                pBsonElement->type() == Object);

        intrusive_ptr<DocumentSourceMatch> pMatcher(
            new DocumentSourceMatch(pBsonElement->Obj(), pExpCtx));

        return pMatcher;
    }

    void DocumentSourceMatch::toMatcherBson(BSONObjBuilder *pBuilder) const {
        const BSONObj *pQuery = matcher.getQuery();
        pBuilder->appendElements(*pQuery);
    }

    DocumentSourceMatch::DocumentSourceMatch(
        const BSONObj &theQuery,
        const intrusive_ptr<ExpressionContext> &pExpCtx):
        DocumentSourceFilterBase(pExpCtx),
        query(theQuery.copy()),
        matcher(query) {
    }

    void DocumentSourceMatch::visitDependencies(
        DependencySink *pSink, const BSONObj *pBsonObj) {
        BSONObjIterator bsonIterator(*pBsonObj);
        while(bsonIterator.more()) {
            BSONElement bsonElement(bsonIterator.next());
            const char *pFieldName = bsonElement.fieldName();

            /* if it's not $or or $and, then it must be a fieldname */
            if (strcmp(pFieldName, "$or") && strcmp(pFieldName, "$and"))
                pSink->dependency(pFieldName);
            else {
                /* visit all the operand objects */
                verify(bsonElement.type() == Array);
                BSONObjIterator logicalOperands(bsonElement.Obj());
                while(logicalOperands.more()) {
                    BSONElement operandElement(logicalOperands.next());
                    BSONObj operand(operandElement.Obj());
                    visitDependencies(pSink, &operand);
                }
            }
        }
    }

    void DocumentSourceMatch::manageDependencies(
        const intrusive_ptr<DependencyTracker> &pTracker) {

        /* collect the dependencies */
        Tracker tracker(pTracker.get(), this);
        visitDependencies(&tracker, &query);
    }

    void DocumentSourceMatch::Tracker::dependency(const string &path) {
        FieldPath fieldPath(path);
        pTracker->addDependency(fieldPath, pSource);
    }

}
