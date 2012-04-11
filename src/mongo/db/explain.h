// @file explain.h - Helper classes for generating query explain output.

/*    Copyright 2012 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include "../pch.h"
#include <boost/shared_ptr.hpp>
#include <list>
#include "../bson/bsonobj.h"
#include "../util/timer.h"

namespace mongo {

    using namespace boost;

    class Cursor;

    /**
     * Note: by default we filter out allPlans and oldPlan in the shell's
     * explain() function. If you add any recursive structures, make sure to
     * edit the JS to make sure everything gets filtered.
     */
    
    /** The timer starts on construction and provides the duration since then or until stopped. */
    class DurationTimer {
    public:
        DurationTimer() : _running( true ), _duration() {}
        void stop() { _running = false; _duration = _timer.millis(); }
        int duration() const { return _running ? _timer.millis() : _duration; }
    private:
        Timer _timer;
        bool _running;
        int _duration;
    };

    class ExplainClauseInfo;

    /** Data describing execution of a query plan. */
    class ExplainPlanInfo {
    public:
        ExplainPlanInfo();

        /** Note information about the plan. */
        void notePlan( const Cursor &cursor, bool scanAndOrder, bool indexOnly );
        /** Note an iteration of the plan. */
        void noteIterate( bool match, bool loadedRecord, const Cursor &cursor );
        /** Note that the plan yielded. */
        void noteYield();
        /** Note that the plan finished execution. */
        void noteDone( const Cursor &cursor );
        /** Note that the plan was chosen over others by the query optimizer. */
        void notePicked();

        /** BSON summary of the plan. */
        BSONObj bson() const;
        /** Combined details of both the plan and its clause. */
        BSONObj pickedPlanBson( const ExplainClauseInfo &clauseInfo ) const;

        bool picked() const { return _picked; }
        bool done() const { return _done; }
        long long n() const { return _n; }
        long long nscanned() const { return _nscanned; }

    private:
        void noteCursorUpdate( const Cursor &cursor );
        string _cursorName;
        bool _isMultiKey;
        long long _n;
        long long _nscannedObjects;
        long long _nscanned;
        bool _scanAndOrder;
        bool _indexOnly;
        int _nYields;
        BSONObj _indexBounds;
        bool _picked;
        bool _done;
        BSONObj _details;
    };
    
    /** Data describing execution of a query clause. */
    class ExplainClauseInfo {
    public:
        ExplainClauseInfo();

        /** Note an iteration of the clause. */
        void noteIterate( bool match, bool loadedRecord, bool chunkSkip );
        /** Revise the total number of documents returned to match an external count. */
        void reviseN( long long n );
        /** Stop the clauses's timer. */
        void stopTimer();

        /** Add information about a plan to this clause. */
        void addPlanInfo( const shared_ptr<ExplainPlanInfo> &info );
        BSONObj bson() const;

        long long n() const { return _n; }
        long long nscannedObjects() const { return _nscannedObjects; }
        long long nscanned() const;
        long long nChunkSkips() const { return _nChunkSkips; }
        int millis() const { return _timer.duration(); }

    private:
        const ExplainPlanInfo &virtualPickedPlan() const;
        std::list<shared_ptr<const ExplainPlanInfo> > _plans;
        long long _n;
        long long _nscannedObjects;
        long long _nChunkSkips;
        DurationTimer _timer;
    };
    
    /** Data describing execution of a query. */
    class ExplainQueryInfo {
    public:
        /** Note an iteration of the query's current clause. */
        void noteIterate( bool match, bool loadedRecord, bool chunkSkip );
        /** Revise the number of documents returned by the current clause. */
        void reviseN( long long n );

        /* Additional information describing the query. */
        struct AncillaryInfo {
            BSONObj _oldPlan;
        };
        void setAncillaryInfo( const AncillaryInfo &ancillaryInfo );
        
        /* Add information about a clause to this query. */
        void addClauseInfo( const shared_ptr<ExplainClauseInfo> &info );
        BSONObj bson() const;

    private:
        static string server();
        
        std::list<shared_ptr<ExplainClauseInfo> > _clauses;
        AncillaryInfo _ancillaryInfo;
        DurationTimer _timer;
    };
    
    /** Data describing execution of a query with a single clause and plan. */
    class ExplainSinglePlanQueryInfo {
    public:
        ExplainSinglePlanQueryInfo();

        /** Note information about the plan. */
        void notePlan( const Cursor &cursor, bool scanAndOrder, bool indexOnly ) {
            _planInfo->notePlan( cursor, scanAndOrder, indexOnly );
        }
        /** Note an iteration of the plan and the clause. */
        void noteIterate( bool match, bool loadedRecord, bool chunkSkip, const Cursor &cursor ) {
            _planInfo->noteIterate( match, loadedRecord, cursor );
            _queryInfo->noteIterate( match, loadedRecord, chunkSkip );
        }
        /** Note that the plan yielded. */
        void noteYield() {
            _planInfo->noteYield();
        }
        /** Note that the plan finished execution. */
        void noteDone( const Cursor &cursor ) {
            _planInfo->noteDone( cursor );
        }

        /** Return the corresponding ExplainQueryInfo for further use. */
        shared_ptr<ExplainQueryInfo> queryInfo() const {
            return _queryInfo;
        }

    private:
        shared_ptr<ExplainPlanInfo> _planInfo;
        shared_ptr<ExplainQueryInfo> _queryInfo;
    };

    /** Interface for recording events that contribute to explain results. */
    class ExplainRecordingStrategy {
    public:
        ExplainRecordingStrategy( const ExplainQueryInfo::AncillaryInfo &ancillaryInfo );
        virtual ~ExplainRecordingStrategy() {}
        /** Note information about a single query plan. */
        virtual void notePlan( bool scanAndOrder, bool indexOnly ) {}
        /** Note an iteration of the query. */
        virtual void noteIterate( bool match, bool orderedMatch, bool loadedRecord,
                                 bool chunkSkip ) {}
        /** Note that the query yielded. */
        virtual void noteYield() {}
        /** @return number of ordered matches noted. */
        virtual long long orderedMatches() const { return 0; }
        /** @return ExplainQueryInfo for a complete query. */
        shared_ptr<ExplainQueryInfo> doneQueryInfo();
    protected:
        /** @return ExplainQueryInfo for a complete query, to be implemented by subclass. */
        virtual shared_ptr<ExplainQueryInfo> _doneQueryInfo() = 0;
    private:
        ExplainQueryInfo::AncillaryInfo _ancillaryInfo;
    };
    
    /** No explain events are recorded. */
    class NoExplainStrategy : public ExplainRecordingStrategy {
    public:
        NoExplainStrategy();
    private:
        /** @asserts always. */
        virtual shared_ptr<ExplainQueryInfo> _doneQueryInfo();
    };
    
    class MatchCountingExplainStrategy : public ExplainRecordingStrategy {
    public:
        MatchCountingExplainStrategy( const ExplainQueryInfo::AncillaryInfo &ancillaryInfo );
    protected:
        virtual void _noteIterate( bool match, bool orderedMatch, bool loadedRecord,
                                  bool chunkSkip ) = 0;
    private:
        virtual void noteIterate( bool match, bool orderedMatch, bool loadedRecord,
                                 bool chunkSkip );
        virtual long long orderedMatches() const { return _orderedMatches; }
        long long _orderedMatches;
    };
    
    /** Record explain events for a simple cursor representing a single clause and plan. */
    class SimpleCursorExplainStrategy : public MatchCountingExplainStrategy {
    public:
        SimpleCursorExplainStrategy( const ExplainQueryInfo::AncillaryInfo &ancillaryInfo,
                                    const shared_ptr<Cursor> &cursor );
    private:
        virtual void notePlan( bool scanAndOrder, bool indexOnly );
        virtual void _noteIterate( bool match, bool orderedMatch, bool loadedRecord,
                                  bool chunkSkip );
        virtual void noteYield();
        virtual shared_ptr<ExplainQueryInfo> _doneQueryInfo();
        shared_ptr<Cursor> _cursor;
        shared_ptr<ExplainSinglePlanQueryInfo> _explainInfo;
    };

} // namespace mongo
