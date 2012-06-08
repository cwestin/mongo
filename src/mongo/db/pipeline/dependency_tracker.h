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

#pragma once

#include "pch.h"

#include <boost/unordered_map.hpp>
#include "mongo/util/intrusive_counter.h"
#include "mongo/db/pipeline/field_path.h"

namespace mongo {

    class BSONObjBuilder;
    class DocumentSource;

    /**
       The dependency tracker is used to analyze a pipeline and determine
       it's dependencies. Dependencies represent data field items that flow
       through the pipeline.

       Control and use of the dependency tracker is from outside, currently
       in PipelineD::prepareCursorSource(). The dependency tracker is used
       to scan the pipeline from its end to its beginning, and each element
       in the pipeline is given a chance to contribute through
       DocumentSource::manageDependencies().

       As the scan of the pipeline's sources progresses, the DependencyTracker
       is expected to contain the current set of dependencies. When the scan
       is complete, what remains should be the set of fields that are required
       from the underlying collection scan that will be the input to the
       pipeline.

       The simplest, most complete example is DocumentSourceGroup, which
       demonstrates the following steps:
       (1) For each field that will be in the source's output, remove that
       from the current set of dependencies. Because the field is a product of
       the source, it satisfies dependencies on it.

       (2) Check to see if there are any remaining dependencies; if there are
       an error can be sent to the user to indicate that there are fields that
       downstream sources need to do their work, but which will not be in
       the result of this source.

       (3) For each field referenced by this source, add a dependency.
       References are found in computed expressions.

       By following these steps, as we move from the end to the beginning of
       the pipeline, we are left with the fields that are needed at the
       beginning of the pipeline.

       Not all sources will use all of these steps. For example, $unwind only
       needs step (3). $unwind does not produce any new fields, but only passes
       through fields. But it does require the field that is to be unwound.
       $sort is similar.  $skip and $limit don't use any of these steps.

       $project is potentially the most complicated, although it is
       conceptually similar to $group. $project is complicated because it
       has computed fields (similar to $group), but can also simply list
       field paths to include. These are treated as products (satisfying
       downstream dependencies), as well as dependencies (inputs to this
       source).

       Once the scan is complete, the DependencyTracker instance should be
       populated with the fields required to satisfy the operation of the
       pipeline. This can be used to reduce the number of fields flowing through
       the pipeline in order to save on memory (e.g., some fields may be large
       binary ones that are not required). This may also be used to support
       index-only queries, by avoiding referencing fields that don't come from
       any index that may be used.

       These options are only possible if the dependency set is "closed."
       MongoDB select-lists offer two modes: inclusionary, and exclusionary.
       When using the inclusionary mode, we can determine which fields are
       required. However, if the exclusionary mode is used, we cannot be
       certain that we don't need all available fields. For example, for
       a $project with some exclusions, followed only by an $unwind, we must
       include all available fields, because we must assume the user may
       reference any of them. This is termedn an "open" dependency set in the
       methods below. Operations such as a $sort do not affect the state of
       this. A $group or a $project in inclusionary mode will cause the set to
       become closed, and once closed, nothing will cause it to be open again.

       In order to avoid circular references, references to DocumentSources
       are raw pointers. The lifetime of the DependencyTracker should be
       within that of the pipeline and its sources. In general, the
       DocumentSource pointers are only used for reporting positions and names
       of sources for errors, via DocumentSource::getPipelineStep(), and
       DocumentSource::getSourceName().
     */
    class DependencyTracker :
        public IntrusiveCounterUnsigned {
    public:
        /**
           Add a dependency.

           Adding a dependency more than once is allowed, and has no effect
           after the first one, other than to replace the previous source with
           the new one. This has the effect of only remembering the most recent
           source of the dependency.

           @param rFieldPath the field that is depended on
           @param pSource the source that depends on it
         */
        void addDependency(const FieldPath &rFieldPath,
                           const DocumentSource *pSource);

        /**
           Remove a dependency.

           Removing a dependency which is not present does nothing.

           @param rFieldPath the field whose dependency is satisfied.
        */
        void removeDependency(const FieldPath &rFieldPath);

        /**
           Check for the existence of a dependency, and if present, reveal
           its source.

           @param rFieldPath the dependency to look up
           @returns the most recent source of the dependency, or NULL if
             there is no such dependency
         */
        const DocumentSource *getDependency(const FieldPath &rFieldPath) const;

        /**
           Report an unsatisifed dependency.

           Mid-way through the analysis of a pipeline, we can detect an
           unsatisfied dependency by noting that after we have removed all
           the satisfied dependencies, there are still dependencies left.

           This can be used to report such situations, and throws a user
           error.

           @param rPath the unsatisfied dependency
           @param pNeeds the source that requires it
           @param pExcludes the source that does not include it as a product
           @throws user error
         */
        static void reportUnsatisfied(
            const FieldPath &rPath, const DocumentSource *pNeeds,
            const DocumentSource *pExcludes);

        /**
           Report the first unsatisfied dependency known.

           Looks at the current set of dependencies, and assumes any
           satisfied ones have already been removed. Calls reportUnsatisifed(),
           on the first one it finds.

           @param pExcludes the source that is currently under consideration;
             this source has removed its satisfied dependencies
         */
        void reportFirstUnsatisfied(const DocumentSource *pExcludes) const;

        /**
           List the current dependencies in textual form.

           Intended for debugging use.

           @param outStream where to write the dependencies to
         */
        void listDependencies(ostream &outStream) const;

        /**
           Build a select-list out of the current set of dependencies.

           Intended to be used at the end of pipeline analysis. This iterates
           over any remaining dependencies, and generates a MongoDB select-list
           (a BSON object using each field as a key, and whose value is
           true).

           @param pBuilder builder to add the select list items to
         */
        void buildSelectList(BSONObjBuilder *pBuilder) const;

        /**
           Note the result set is closed.

           Result fields are assumed to be an open set to start, but can be
           closed through when pipeline segments appear that have explicit
           output, such as a $project in inclusion mode, or a $group.
         */
        void setClosedSet();

        /**
           Find out if the result set is closed.

           @returns true if the result set is closed, false otherwise
         */
        bool isClosedSet() const;

        /**
           Factory function.

           @returns a new empty DependencyTracker
         */
        static intrusive_ptr<DependencyTracker> create();

    private:
        DependencyTracker();

        bool openSet; // open/closed result set

        struct Tracker {
            Tracker(const FieldPath &rFieldPath,
                    const DocumentSource *pSource);

            FieldPath fieldPath; // dependency
            const DocumentSource *pSource; // source of the dependency

            // hashing for the dependency map
            struct Hash :
                unary_function<string, size_t> {
                size_t operator()(const FieldPath &rFieldPath) const;
            };
        };

        /* the dependency map */
        typedef boost::unordered_map<FieldPath, Tracker, Tracker::Hash> MapType;
        MapType map;
    };

}


/* ======================= PRIVATE IMPLEMENTATIONS ========================== */

namespace mongo {

    inline void DependencyTracker::setClosedSet() {
        openSet = false;
    }

    inline bool DependencyTracker::isClosedSet() const {
        return !openSet;
    }

}
