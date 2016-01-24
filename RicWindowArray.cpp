#include <sys/time.h>
#include <math.h>


#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "RicWindowArray.h"



namespace scidb 
{

using namespace std;
using namespace boost;


//	* RicWindowChunkIterator -------------------------------------------------------------

	RicWindowChunkIterator::RicWindowChunkIterator(RicWindowArrayIterator const& arrayIterator, RicWindowChunk const& chunk, int mode):
		_arrayIter(arrayIterator),
		_array(arrayIterator.array),
		_chunk(chunk),
		_firstPos(_chunk.getFirstPosition(false)),
		_lastPos(_chunk.getLastPosition(false)),
		_nDims(_firstPos.size()),
		_window(_array._window),
		_currPos(_nDims),
		_movePos(_nDims),
		_chunkSize(_nDims),
		_windowSize(_nDims),
		_attrID(_chunk._attrID),
		_aggregate(_array._aggregates[_attrID]->clone()),
		_defaultValue(_chunk.getAttributeDesc().getDefaultValue()),
		_iterationMode(mode),
		_nextValue(TypeLibrary::getType(_chunk.getAttributeDesc().getType()))
	{

		int iterMode = IGNORE_EMPTY_CELLS;
		if (_aggregate->ignoreNulls())
		{
			_noNullsCheck = true;
			iterMode |= IGNORE_NULL_VALUES;
		}
		else {
			_noNullsCheck = false;
		}
		_inputIterator = arrayIterator.iterator->getChunk().getConstIterator(iterMode);

		if (_array.getArrayDesc().getEmptyBitmapAttribute()) 		//empty ?
		{
			AttributeID eAttrId = _array._inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
			_emptyTagArrayIterator = _array._inputArray->getConstIterator(eAttrId);

			if (!_emptyTagArrayIterator->setPosition(_firstPos))
				throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
			_emptyTagIterator = _emptyTagArrayIterator->getChunk().getConstIterator(IGNORE_EMPTY_CELLS);
		}

		//--------------------------------------------------------------------------
		//initialize the bufferSet;

		_aggrType = getAggrType(_aggregate->getName());

		size_t numBuf = 1;
		for (size_t i = 0; i < _nDims; i++) {
			_windowSize[i] = _window[i]._boundaries.first + _window[i]._boundaries.second + 1;
			_chunkSize[i] = _lastPos[i] - _firstPos[i] + 1;
		}
		for (int i = _nDims-1; i >= 0; i--) {
			vector< BufferTool*> curVec;
			for (size_t j = 0; j < numBuf; j++) {
				curVec.push_back(createSpecificBuffer(_windowSize[i]));
				//curVec.push_back(new SumBuffer(_windowSize[i]));
			}
			_bufferSet.push_back(curVec);
			numBuf *= _chunkSize[i];
		}
		reset();
	}


	RicWindowChunkIterator::~RicWindowChunkIterator() {
		// release the memory allocated for the buffers
		for (size_t i = 0; i < _nDims; i++) {
			for (size_t j = 0; j < _bufferSet[i].size(); j++) {
				//releaseSpecificBuffer(_bufferSet[i][j]);
				delete _bufferSet[i][j];
			}
		}	
	}

	BufferTool *RicWindowChunkIterator::createSpecificBuffer(size_t size) {
//		return new SumBuffer(size);
		switch (_aggrType) {
			default		:	return NULL;
			case SUM	:	return new SumBuffer(size);
			case AVG	:	return new AvgBuffer(size);
			case MIN	:	return new MinQueue(size);
			case MAX	:	return new MaxQueue(size);
			case VAR	:	return new VarBuffer(size);
			case STDEV	:	return new VarBuffer(size);
		}	
	}



	aggrType RicWindowChunkIterator::getAggrType(string const& name)
	{
		if (name == "count")
			return SUM;
		else if (name == "sum")
			return SUM;
		else if (name == "avg")
			return AVG;
		else if (name == "min")
			return MIN;
		else if (name == "max")
			return MAX; 
		else if (name == "var") {
			return VAR;
		} else if (name == "stdev") {
			return STDEV;
		} else
			return SUM;
	}

	int RicWindowChunkIterator::getMode()
	{
		return _iterationMode;
	}


	//	calculating the whole window
	Value& RicWindowChunkIterator::calculateNextValue()
	{

		Coordinates firstGridPos(_nDims);
		Coordinates lastGridPos(_nDims);

		// get the window grid scope
		for (size_t i = 0; i < _nDims; i++) {
			/*
			firstGridPos[i] = std::max(_currPos[i] - _chunk._array._window[i]._boundaries.first,
					_chunk._array._dimensions[i].getStartMin());
			lastGridPos[i] = std::min(_currPos[i] + _chunk._array._window[i]._boundaries.second,
					_chunk._array._dimensions[i].getEndMax());
					*/
			firstGridPos[i] = _currPos[i] - _window[i]._boundaries.first;
			lastGridPos[i] = _currPos[i] + _window[i]._boundaries.second;
		}

		

		int startDim = _nDims - 1;
		while (startDim >= 0 && _currPos[startDim] == _firstPos[startDim]) {
			startDim--;		
		}
		startDim++;



		for (size_t i = startDim; i < _nDims; i++) {
			for (size_t j = 0; j < i; j++)
				_movePos[j] = lastGridPos[j];
			prepare(i, firstGridPos[i], lastGridPos[i] - 1);
			/*
			prepare(i, 
					_firstPos[i] - _window[i]._boundaries.first,
					_firstPos[i] + _window[i]._boundaries.second - 1);*/
		}

		if (_bufferSet[1][0] < 0 || _bufferSet[1][1] < 0) {
			startDim = 1;
		}

		BufferTool* curBuf;
		double curValue = accessValue(lastGridPos);
		int curDim = _nDims-1;
		size_t offset = 0;
		size_t curNum = 1;
		double curSum2 = curValue*curValue;
		

		while (curDim >= 0) {
			curBuf = _bufferSet[_nDims - curDim - 1][offset];
			if (_currPos[curDim] != _firstPos[curDim])
				curBuf->remove();

			if (_aggrType == VAR || _aggrType == STDEV) {
				curBuf->insert(curValue, curNum, curSum2);
				curSum2 = curBuf->getSum2();
			} else {
				curBuf->insert(curValue, curNum);
			}

			curValue = curBuf->getCurrent();
			curNum = curBuf->getNum();
			// offset should be computed from the local position in the chunk 
			offset = offset * _chunkSize[curDim] + ( _currPos[curDim] - _firstPos[curDim] );
			curDim--;
		}

		
		double v = curValue;
		if (_aggrType == VAR || _aggrType == STDEV) {
			if (curNum == 1) {
				v = -1;
			} else {
				v = (curSum2 - curValue*curValue/curNum)/(curNum-1);
				if (_aggrType == STDEV)
					v = sqrt(v);
			}
		}

		_nextValue.setData(&v, sizeof(double));
		return _nextValue;
		
		
		
		/*
		struct timeval cur;
		gettimeofday(&cur, NULL);
		//size_t v = cur.tv_sec%10*1000+cur.tv_usec/1000;
		double v = cur.tv_sec%10*1000000+cur.tv_usec;
		_nextValue.setData(&v, sizeof(double));

		//for test
		if (_inputIterator->setPosition(_currPos))
		{	
			_nextValue.setData(&number, sizeof(double));
			//_nextValue = _inputIterator->getItem();
		}		
		*/
	}


	double RicWindowChunkIterator::accessValue(Coordinates coor) {
		if (_inputIterator->setPosition(coor)) {
			return ValueToDouble(_aggregate->getAggregateType().typeId() , _inputIterator->getItem());
		} else {
			return NULL_VALUE;
		}
	}


	void RicWindowChunkIterator::prepare(size_t dim, int start, int end) {
		for (size_t i = 0; i < _bufferSet[_nDims - dim - 1].size(); i++)
			_bufferSet[_nDims - dim - 1][i]->clear();

		_movePos[dim] = start;
		while (_movePos[dim] <= end) {
			if (dim == _nDims - 1) {
				double value = accessValue(_movePos);
				recursiveUpdate(value);
			} else {
				prepare(dim + 1, 
						_firstPos[dim+1] - _window[dim+1]._boundaries.first, 
						_lastPos[dim+1] + _window[dim+1]._boundaries.second);
			}
			_movePos[dim]++;
		}
	}



	void RicWindowChunkIterator::recursiveUpdate(double v) {
		BufferTool* curBuf;
		double curValue = v;
		size_t curNum = 1;
		double curSum2 = v*v;

		int curDim = _nDims-1;
		int fullWindow;
		size_t offset = 0;


		while (curDim >= 0) {
			curBuf = _bufferSet[_nDims - curDim - 1][offset];
			fullWindow = _firstPos[curDim] + _window[curDim]._boundaries.second;
			
			if (_movePos[curDim] > fullWindow)
				curBuf->remove();

			if (_aggrType == VAR || _aggrType == STDEV) {
				curBuf->insert(curValue, curNum, curSum2);
				curSum2 = curBuf->getSum2();
			} else {
				curBuf->insert(curValue, curNum);
			}
			if (_movePos[curDim] <  fullWindow)
				break;
			curValue = curBuf->getCurrent();
			curNum = curBuf->getNum();
			offset = offset * _chunkSize[curDim] + ( _movePos[curDim] - _window[curDim]._boundaries.second - _firstPos[curDim] );
			curDim--;
		}
	}




	Value& RicWindowChunkIterator::getItem()
	{
		if (!_hasCurrent)
			throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
		return _nextValue;
	}

	Coordinates const& RicWindowChunkIterator::getPosition()
	{
		return _currPos;
	}

	bool RicWindowChunkIterator::setPosition(Coordinates const&pos)
	{
		for (size_t i = 0; i < _currPos.size(); i++)
		{
			if (pos[i] < _firstPos[i] || pos[i] > _lastPos[i])
				return false;
		}
		_currPos = pos;
		number += 1000;
		calculateNextValue();
		if (_iterationMode & IGNORE_NULL_VALUES && _nextValue.isNull())
			return false;
		if (_iterationMode & IGNORE_DEFAULT_VALUES && _nextValue == _defaultValue)
			return false;

		return true;
	}


	bool RicWindowChunkIterator::isEmpty()
	{
		return false;
	}

	void RicWindowChunkIterator::reset()
	{
		if (setPosition(_firstPos))
		{
			_hasCurrent = true;
			return;
		}
		++(*this);
	}

	void RicWindowChunkIterator::operator ++()
	{
		bool done = false;
		while (!done)
		{
			size_t nDims = _firstPos.size();
			for (size_t i = nDims-1; ++_currPos[i] > _lastPos[i]; i--)
			{
				if (i == 0)
				{
					_hasCurrent = false;
					return;
				}
				_currPos[i] = _firstPos[i];
			}
			number += 1;
			calculateNextValue();
			
			if (_iterationMode & IGNORE_NULL_VALUES && _nextValue.isNull())
				continue;
			if (_iterationMode & IGNORE_DEFAULT_VALUES && _nextValue == _defaultValue)
				continue;
			done = true;
			_hasCurrent = true;
		}

	}

	bool RicWindowChunkIterator::end()
	{
		return !_hasCurrent;
	}

	ConstChunk const& RicWindowChunkIterator::getChunk()
	{
		return _chunk;
	}



//	* RicWindowChunk ------------------------------------------------------------------------
	RicWindowChunk::RicWindowChunk(RicWindowArray const& arr, AttributeID attr):
		_array(arr),
		_arrayIterator(NULL),
		_nDims(arr._desc.getDimensions().size()),
		_firstPos(_nDims),
		_lastPos(_nDims),
		_attrID(attr)
	{
		if (arr._desc.getEmptyBitmapAttribute() == 0 || attr != arr._desc.getEmptyBitmapAttribute()->getId())
			_aggregate = arr._aggregates[_attrID]->clone();
	}

	Array const& RicWindowChunk::getArray() const
	{
		return _array;
	}


	const ArrayDesc& RicWindowChunk::getArrayDesc() const
	{
		return _array._desc;
	}

	const AttributeDesc& RicWindowChunk::getAttributeDesc() const
	{
		return _array._desc.getAttributes()[_attrID];
	}

	Coordinates const& RicWindowChunk::getFirstPosition(bool withOverlap) const
	{
		return _firstPos;
	}

	Coordinates const& RicWindowChunk::getLastPosition(bool withOVerlap) const
	{
		return _lastPos;
	}

	shared_ptr<ConstChunkIterator> RicWindowChunk::getConstIterator(int iterationMode) const
	{
		SCIDB_ASSERT( (_arrayIterator != NULL) );
		ConstChunk const& inputChunk = _arrayIterator->iterator->getChunk();	//chunk in SrcArr
		
		if (_array.getArrayDesc().getEmptyBitmapAttribute() && _attrID == _array.getArrayDesc().getEmptyBitmapAttribute()->getId())
		{
			return inputChunk.getConstIterator((iterationMode & ~ChunkIterator::INTENDED_TILE_MODE) | ChunkIterator::IGNORE_OVERLAPS);
		}	
		// materiliezed?

		return shared_ptr<ConstChunkIterator>(new RicWindowChunkIterator(*_arrayIterator, *this, iterationMode));
	}

	int RicWindowChunk:: getCompressionMethod() const
	{
		return _array._desc.getAttributes()[_attrID].getDefaultCompressionMethod();
	}


	void RicWindowChunk::setPosition(RicWindowArrayIterator const* iterator, Coordinates const& pos)
	{
		_arrayIterator = iterator;
		_firstPos = pos;
		Dimensions const& dims = _array._desc.getDimensions();

		for (size_t i = 0; i < dims.size(); i++)
		{
			_lastPos[i] = _firstPos[i] + dims[i].getChunkInterval() - 1;
			if (_lastPos[i] > dims[i].getEndMax())
				_lastPos[i] = dims[i].getEndMax();

		}
		if (_aggregate.get() == 0)
			return;
		
		if (_array._desc.getEmptyBitmapAttribute())
		{
		}
	}







//	* RicWindowArrayIterator ---------------------------------------------------------------------


	RicWindowArrayIterator::RicWindowArrayIterator(RicWindowArray const& arr, AttributeID attrID, AttributeID input):
		array(arr),
		iterator(arr._inputArray->getConstIterator(input)),
		currPos(arr._dimensions.size()),
		chunk(arr, attrID)
	{
		number = 0;
		reset();
	}
		
	ConstChunk const& RicWindowArrayIterator::getChunk()
	{
		if (!chunkInitialized)
		{
			chunk.setPosition(this, currPos);
			chunkInitialized = true;
		}
		return chunk;
	}

	Coordinates const& RicWindowArrayIterator::getPosition()
	{
		if (!hasCurrent)
			throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
		return currPos;
	}


	bool RicWindowArrayIterator::setPosition(Coordinates const& pos)
	{
		chunkInitialized = false;					//?
		if (!iterator->setPosition(pos))
		{
			return hasCurrent = false;
		}
		currPos = pos;
		return hasCurrent = true;
	}


	bool RicWindowArrayIterator::end()
	{
		return !hasCurrent;
	}

	void RicWindowArrayIterator::operator ++()
	{
		if (!hasCurrent)
			throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
		chunkInitialized = false;					//?
		++(*iterator);
		number += 1;
		hasCurrent = !iterator->end();
		if (hasCurrent)
		{
			currPos = iterator->getPosition();
		}
	}

	void RicWindowArrayIterator::reset()
	{
		chunkInitialized = false;
		iterator->reset();
		hasCurrent = !iterator->end();
		if (hasCurrent)
		{
			currPos = iterator->getPosition();
		}
	}


//	* RicWindowArray     	---------------------------------------------------------------------

	RicWindowArray::RicWindowArray(ArrayDesc const& desc, shared_ptr<Array> const& inputArray,
			vector<WindowBoundaries> const& window, vector<AttributeID> const& inputAttrIDs,
			vector<AggregatePtr> const& aggregates):
		_desc(desc),
		_inputDesc(inputArray->getArrayDesc()),
		_window(window),
		_dimensions(_desc.getDimensions()),
		_inputArray(inputArray),
		_inputAttrIDs(inputAttrIDs),
		_aggregates(aggregates)
	{}

	ArrayDesc const& RicWindowArray::getArrayDesc() const
	{
		return _desc;
	}

	shared_ptr<ConstArrayIterator> RicWindowArray::getConstIterator(AttributeID attr) const
	{
		if (_desc.getEmptyBitmapAttribute() && attr == _desc.getEmptyBitmapAttribute()->getId())
		{
			return shared_ptr<ConstArrayIterator>(new RicWindowArrayIterator(*this, attr, _inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId()));
		}
		return shared_ptr<ConstArrayIterator>(new RicWindowArrayIterator(*this, attr, _inputAttrIDs[attr] ));
	}


} // namespace

