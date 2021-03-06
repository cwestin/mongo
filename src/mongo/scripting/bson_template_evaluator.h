/** @file templateevaluator.h */

/*
 *    Copyright (C) 2012 10gen Inc.
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

/*
 * This library supports a templating language that helps in generating BSON documents from a
 * template. The language supports the following template:
 * #RAND_INT #LITERAL, #CONCAT, #RAND_STRING.
 *
 * The language will help in quickly expressing richer documents  for use in benchRun.
 * Ex. : { key : { #RAND_INT: [10, 20] } } or  { key : { #CONCAT: ["hello", " ", "world"] } }
 *
 * Where possible, the templates can also be combined together and evaluated. For eg.
 * { key : { #CONCAT: [{ #RAND_INT: [10, 20] }, " ", "world"] } }
 *
 * This library DOES NOT support combining or nesting the templates in an arbitrary fashion.
 * eg. { key : { #RAND_INT: [{ #RAND_INT: [10, 15] }, 20] } } is not supported.
 *
 */
#pragma once

#include <map>
#include <string>

#include <boost/function.hpp>
#include <boost/noncopyable.hpp>

#include "mongo/db/jsobj.h"

namespace mongo {

    /*
     * BsonTemplateEvaluator Object for evaluating the templates. The Object exposes
     * methods to evaluate existing template operators (#RAND_INT) and add new template operators.
     *
     * To evaluate a template, call the object's 'evaluate' method and pass it as arguments, the
     * template object that you want to evaluate and a BSONObjBuilder object that will contain the
     * resultant BSON object. Eg.
     *
     * Status st = bsonTemplEvalObj->evaluate(inputTemplateObj, outputBSONObjBuilder)
     *
     * The 'evaluate' method will never throw an exception and will return an appropriate Status
     * code on success/error scenario.
     *
     * High level working : The evaluate() method takes in a BSONObj as input, iterates over the
     * BSON elements in the input BSONObj, and calls _evalElem() method. The _evalElem() method
     * figures out the specific template and then calls the corresponding template function.
     * The evaluated result is added to the BSONObjBuilder object and is returned to the evaluate()
     * method.
     *
     */
    class BsonTemplateEvaluator : private boost::noncopyable {
    public:
        /* Status of template evaluation. Logically the  the status are "success", "bad operator"
         * and "operation evaluation error." */
        enum Status {
            StatusSuccess = 0,
            StatusBadOperator,
            StatusOpEvaluationError
        };

        /*
         * OperatorFn : function object wrappers that define a call interface.
         * All template operators have this signature.
         * @params btl : pointer to the BsonTemplateEvaluator Object
         *         fieldName : key of the object being evaluated
         *         in : the embedded BSONObj
         *         builder : the output BSONObj
         *  Eg. for object { key : { #RAND_INT: [10, 20] } }
         *      fieldName : key
         *      in : { #RAND_INT: [10, 20] }
         */
        typedef boost::function< Status (BsonTemplateEvaluator* btl, const char* fieldName,
                                         const BSONObj& in, BSONObjBuilder& builder) > OperatorFn;

        BsonTemplateEvaluator();
        ~BsonTemplateEvaluator();

        /*
         * "Add a new operator, "name" with behavior "op" to this evaluator.
         */
        void addOperator(const std::string& name, const OperatorFn& op);

        /*
         * Returns the OperatorFn registered for the operator named "op", or
         * OperatorFn() if there is no such operator.
         */
        OperatorFn operatorEvaluator(const std::string& op) const;

        /* This is the top level method for using this library. It takes a BSON Object as input,
         * evaluates the templates and saves the result in the builder object.
         * The method returns a status code on success/error condition.
         * The templates cannot be used at the top level.
         * So this is okay as an input { key : {#RAND_INT: [10, 20]} }
         * but not this { {#RAND_INT : [10, 20]} : some_value }
         */
        Status evaluate(const BSONObj& src, BSONObjBuilder& builder);

    private:
        void initializeEvaluator();
        // map that holds operators along with their respective function pointers
        typedef std::map< std::string, OperatorFn > OperatorMap;
        OperatorMap _operatorFunctions;

        // evaluates a BSON element. This is internally called by the top level evaluate method.
        Status _evalElem(BSONElement in, BSONObjBuilder& out);

        // operator methods
        static Status evalRandInt(BsonTemplateEvaluator* btl, const char* fieldName,
                                  const BSONObj in, BSONObjBuilder& out);

    };

} // end namespace
