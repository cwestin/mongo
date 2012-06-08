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
#include "mongo/db/pipeline/dependency_tracker.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/document_source.h"

namespace mongo {

    DependencyTracker::DependencyTracker():
        openSet(true),
        map() {
    }

    intrusive_ptr<DependencyTracker> DependencyTracker::create() {
        return new DependencyTracker();
    }

    DependencyTracker::Tracker::Tracker(
        const FieldPath &rFieldPath, const DocumentSource *pS):
        fieldPath(rFieldPath),
        pSource(pS) {
    }

    size_t DependencyTracker::Tracker::Hash::operator()(
        const FieldPath &rFieldPath) const {
        size_t seed = 0xf0afbeef;
        rFieldPath.hash_combine(seed);
        return seed;
    }

    void DependencyTracker::addDependency(
        const FieldPath &rFieldPath, const DocumentSource *pSource) {
        Tracker tracker(rFieldPath, pSource);
        std::pair<MapType::iterator, bool> p(
            map.insert(std::pair<FieldPath, Tracker>(rFieldPath, tracker)));

        /*
          If there was already an entry, update the dependency to be the more
          recent source.
        */
        if (!p.second)
            (*p.first).second.pSource = pSource;

        DEV {
            (log() << "\n---- DependencyTracker::addDependency(" <<
             rFieldPath.getPath(false) << ", pipeline[" <<
             pSource->getPipelineStep() << "]." <<
             pSource->getSourceName() << ")").flush();
        }
    }

    void DependencyTracker::removeDependency(const FieldPath &rFieldPath) {
        const size_t nErased = map.erase(rFieldPath);

        DEV {
            (log() << "\n---- DependencyTracker::removeDependency(" <<
             rFieldPath.getPath(false) << ") -> " << nErased << "\n").flush();
        }
    }

    const DocumentSource *DependencyTracker::getDependency(
        const FieldPath &rFieldPath) const {
        MapType::const_iterator i(map.find(rFieldPath));
        if (i == map.end())
            return NULL;

        return (*i).second.pSource;
    }

    void DependencyTracker::reportUnsatisfied(
        const FieldPath &rPath,
        const DocumentSource *pNeeds,
        const DocumentSource *pExcludes) {

        uassert(15984, str::stream() <<
                "unable to satisfy dependency on " <<
                rPath.getPath(true) << " in pipeline[" <<
                pNeeds->getPipelineStep() << "]." <<
                pNeeds->getSourceName() << ", because pipeline[" <<
                pExcludes->getPipelineStep() << "]." <<
                pExcludes->getSourceName() << "] doesn't include it",
                false); // printf() is way easier to read than this crap
    }

    void DependencyTracker::reportFirstUnsatisfied(
        const DocumentSource *pExcludes) const {
        for(MapType::const_iterator depIter(map.begin());
            depIter != map.end(); ++depIter) {
            reportUnsatisfied(
                (*depIter).second.fieldPath, (*depIter).second.pSource,
                pExcludes);
        }
    }

    void DependencyTracker::listDependencies(ostream &outStream) const {
        const char *pStatus = openSet ? "open" : "closed";
        outStream << "---- DependencyTracker::listDependencies() (" <<
            pStatus << "):\n";

        for(MapType::const_iterator iter(map.begin());
            iter != map.end(); ++iter) {
            (*iter).first.writePath(outStream, false);
            outStream << " from pipeline[" <<
                (*iter).second.pSource->getPipelineStep() << "]." <<
                (*iter).second.pSource->getSourceName() << "\n";
        }
        outStream << "----\n";
    }

    void DependencyTracker::buildSelectList(BSONObjBuilder *pBuilder) const {
        verify(!openSet);

        /* add all the fields to the builder (in pseudo-random order) */
        for(MapType::const_iterator iter(map.begin());
            iter != map.end(); ++iter) {
            stringstream ss;
            (*iter).first.writePath(ss, false);
            pBuilder->append(ss.str(), true);
        }
    }

}
