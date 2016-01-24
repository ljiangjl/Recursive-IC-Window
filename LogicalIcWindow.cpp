/*
compute the window aggregate incrementally.

*/

#include <boost/shared_ptr.hpp>
#include <log4cxx/logger.h>

#include <query/Operator.h>
#include <system/Exceptions.h>
#include "IcWindowArray.h"


namespace scidb 
{

using namespace std;

/*
 * @Synopsis:
 * ic_window()
 * ic_window( srcArray {, leftEdge, rightEdge} {, AGGREGATE_CALL} )
 * AGGREGATE_CALL := AGGREGATE_FUNC(intputAttr) [as resultName]
 * AGGREGATE_FUNC := sum | avg | min | max | count | stdev | var 
 *
 * @output array:
 * < aggregate calls' resultNames>
 * [ srcDims ]
*/

class LogicalIcWindow : public LogicalOperator
{
public:

    LogicalIcWindow(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
		// input an array 
    	ADD_PARAM_INPUT();
		// followed by a variable list of paramaters.	
		ADD_PARAM_VARIES();
    }


	// Given the schemas of input array, the parameters supplied so far, return a list of all possible types of the next parameter
	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc>& schemas)
	{
		// There must be as many {, leftEdge, rightEdge} pairs as there are dimensions in srcArray
		// There must be at least one aggregate_call

		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;

		if ( _parameters.size() < schemas[0].getDimensions().size()*2 ) 		//still window boundaries
			res.push_back(PARAM_CONSTANT("int64"));
		else if ( _parameters.size() == schemas[0].getDimensions().size()*2 )		//window boundaries finished, aggregates start
			res.push_back(PARAM_AGGREGATE_CALL());
		else {
			res.push_back(PARAM_AGGREGATE_CALL());
			res.push_back(END_OF_VARIES_PARAMS());
		}
		return res;
	}



	//param desc --> the input array schema
	inline ArrayDesc createWindowDesc(ArrayDesc const& desc)
	{
		//get dimensions for output array
		Dimensions const& dims = desc.getDimensions();
		Dimensions aggrDims(dims.size());
		for (size_t i = 0; i < dims.size(); i++)
		{
			DimensionDesc const& srcDim = dims[i];
			aggrDims[i] = DimensionDesc(srcDim.getBaseName(),
									    srcDim.getNamesAndAliases(),
								   	    srcDim.getStartMin(),
									    srcDim.getCurrStart(),
									    srcDim.getCurrEnd(),
									    srcDim.getEndMax(),
									    srcDim.getChunkInterval(),
									    0);
		}

		ArrayDesc output(desc.getName(), Attributes(), aggrDims);
		
		//get the aggregates, check if they make sense, make attributes for output array	
		//_parameters[0~dims.size()*2-1] --> window boundaries, already get in inferSchema
		for (size_t i = dims.size()*2; i < _parameters.size(); i++)
		{
			boost::shared_ptr<scidb::OperatorParam> param = _parameters[i];
			
			if ( param->getParamType() != PARAM_AGGREGATE_CALL) {
				throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
										   SCIDB_LE_OP_WINDOW_ERROR5,
										   _parameters[i]->getParsingContext());
			}
			addAggregatedAttribute( (shared_ptr<OperatorParamAggregateCall> &) param, desc, output, true);
		}

		if ( desc.getEmptyBitmapAttribute())			//?
		{
			AttributeDesc const* eAttr = desc.getEmptyBitmapAttribute();
			output.addAttribute(AttributeDesc(output.getAttributes().size(), 
						eAttr->getName(),
						eAttr->getType(),
						eAttr->getFlags(),
						eAttr->getDefaultCompressionMethod()));
		}

		return output;
	}



	// output array schema
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
		// input array to be only one.
    
		SCIDB_ASSERT(schemas.size() == 1);

		ArrayDesc const& desc = schemas[0];
		size_t nDims = desc.getDimensions().size();
		vector<WindowBoundaries> window(nDims);
		size_t windowSize = 1;
		for (size_t i = 0, size = nDims * 2, boundaryNo = 0; i < size; i+=2, ++boundaryNo)
		{
			int64_t boundaryLower = 
					evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(), query, TID_INT64).getInt64();
			
			//if (boundaryLower < 0)
		
			int64_t boundaryUpper = 	
					evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i+1])->getExpression(), query, TID_INT64).getInt64();

			//if (boundaryUpper < 0)

			window[boundaryNo] = WindowBoundaries(boundaryLower, boundaryUpper);
			windowSize *= window[boundaryNo]._boundaries.second + window[boundaryNo]._boundaries.first + 1; 
		}
		if (windowSize == 1)
			throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR4,
				_parameters[0]->getParsingContext());

        return createWindowDesc(desc);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalIcWindow, "ic_window");

} //namespace scidb
