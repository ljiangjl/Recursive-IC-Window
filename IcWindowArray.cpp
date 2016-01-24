


#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "IcWindowArray.h"


double defaultResult[5] = {0,0,0,1e10,-1e10};

namespace scidb 
{

using namespace std;
using namespace boost;

//  * IncrementalComputer  ----------------------------------------------------------

	void IncrementalComputer::baseWindowInitialize()
	{
		if (_aggrType < 3 )			// sum avh
		{
			_headPos = 0;
			_currPos = 0;
			_sum = 0;
		}
		else if (_aggrType < 5) {	// min max
			while (!_heap.empty())
			{
				_heap.pop();
			}
		}

	}

	void IncrementalComputer::removeOld()
	{
		if (_aggrType < 3)
		{
			_sum -= _interBuf[_headPos];
			_headPos++;
			if (_headPos == _bufferSize)
				_headPos = 0;
		}
	}

	void IncrementalComputer::insertNew(int pos, double newValue)
	{
		if (_aggrType < 3)
		{
			_interBuf[_currPos] = newValue;
			_currPos++;
			if (_currPos == _bufferSize)
				_currPos = 0;
			_sum += newValue;
		}
		else if (_aggrType == 3) //min
		{
			_heap.push(PriorityQueueNode(pos, -newValue));
		}
		else if (_aggrType == 4) //max
		{
			_heap.push(PriorityQueueNode(pos, newValue));
		}
	}

	double IncrementalComputer::calculateCurrentValue(int pos)
	{
		if (_aggrType == 0)
			return _windowSize;
		else if (_aggrType == 1)
			return _sum;
		else if (_aggrType == 2)
			return _sum/_windowSize;
		else {
			while (_heap.top()._pos < pos)
				_heap.pop();
			if (_aggrType == 3)
				return _heap.top()._value*(-1.0);
			else 
				return _heap.top()._value;
		}
	}




//	* IcWindowChunkIterator -------------------------------------------------------------

	IcWindowChunkIterator::IcWindowChunkIterator(IcWindowArrayIterator const& arrayIterator, IcWindowChunk const& chunk, int mode):
		_array(arrayIterator.array),
		_chunk(chunk),
		_firstPos(_chunk.getFirstPosition(false)),
		_lastPos(_chunk.getLastPosition(false)),
		_currPos(_firstPos.size()),
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
		size_t nDims = _firstPos.size();
		_IcWorker._bufferSize = _chunk._array._window[nDims-1]._boundaries.first + _chunk._array._window[nDims-1]._boundaries.second + 1;
		_IcWorker._interBuf = new double[_IcWorker._bufferSize];
		 
		_IcWorker._aggrType = getAggrTypeFromName(_aggregate->getName());
		reset();
	}
		
	int IcWindowChunkIterator::getAggrTypeFromName(string const& name)
	{
		if (name == "count")
			return 0;
		else if (name == "sum")
			return 1;
		else if (name == "avg")
			return 2;
		else if (name == "min")
			return 3;
		else if (name == "max")
			return 4;
		return -1;
	}

	int IcWindowChunkIterator::getMode()
	{
		return _iterationMode;
	}

	void IcWindowChunkIterator::accumulate(Value const& v)
	{
		double value = ValueToDouble(_aggregate->getAggregateType().typeId() ,v);
		if (_IcWorker._aggrType == 0)
			return;
		else if (_IcWorker._aggrType < 3)  //_aggregate->getName() == "sum" || _aggregate->getName() == "avg")
		{
			_result += value;
		}
		else if (_IcWorker._aggrType == 3)  // min
		{
			if (value < _result) _result = value;
		}
		else if (_IcWorker._aggrType == 4)  // max
		{
			if (value > _result) _result = value;
		}
	}


	// the calculated result is stored in _result;
	void IcWindowChunkIterator::calculateWindowUnit(Coordinates const& first, Coordinates const& last, int lastDimValue)
	{
		size_t nDims = first.size();
		Coordinates currPos(nDims);
		currPos = first;
		currPos[nDims-1] = lastDimValue;

		_result = defaultResult[_IcWorker._aggrType];
		if (nDims == 1) {
			if (_inputIterator->setPosition(currPos))
			{
				Value& v = _inputIterator->getItem();
				accumulate(v);
			}
			return;
		}


		currPos[nDims-2] -= 1;
		while (true)
		{
			for (size_t i =  nDims-2; ++currPos[i] > last[i]; i--)
			{
				if (i == 0)
				{
					return;
				}
				currPos[i] = first[i];
			}

			if (_inputIterator->setPosition(currPos))
			{
				Value& v = _inputIterator->getItem();

				if (_noNullsCheck)
				{
					if (v.isNull())
						continue;
				}
				accumulate(v);
			}

		}
	}

	//	calculating the whole window
	Value& IcWindowChunkIterator::calculateNextValue()
	{
		size_t nDims = _currPos.size();
		Coordinates firstGridPos(nDims);
		Coordinates lastGridPos(nDims);

		// get the window grid scope
		for (size_t i = 0; i < nDims; i++) {
			firstGridPos[i] = std::max(_currPos[i] - _chunk._array._window[i]._boundaries.first,
					_chunk._array._dimensions[i].getStartMin());
			lastGridPos[i] = std::min(_currPos[i] + _chunk._array._window[i]._boundaries.second,
					_chunk._array._dimensions[i].getEndMax());
		}

		_IcWorker._windowSize = 1;
		for (size_t i = 0; i < nDims; i++)
			_IcWorker._windowSize *= lastGridPos[i] - firstGridPos[i] + 1;

		if (_currPos[nDims-1] == _firstPos[nDims-1])
		{
			_IcWorker.baseWindowInitialize();
	
			for (int i = firstGridPos[nDims-1]; i <= lastGridPos[nDims-1]; i++)
			{
				calculateWindowUnit(firstGridPos, lastGridPos, i);
				_IcWorker.insertNew(i, _result);
			}
		}
		else {
			if (_firstPos[nDims-1] != firstGridPos[nDims-1])
				_IcWorker.removeOld();

			int realLastPos = _currPos[nDims-1] + _chunk._array._window[nDims-1]._boundaries.second;
			if (realLastPos == lastGridPos[nDims-1])
			{
				calculateWindowUnit(firstGridPos, lastGridPos, lastGridPos[nDims-1]);
				_IcWorker.insertNew(lastGridPos[nDims-1], _result);
			}
		}

		double v = _IcWorker.calculateCurrentValue(firstGridPos[nDims-1]);
		_nextValue.setData(&v, sizeof(double));

		//for test
		/*
		if (_inputIterator->setPosition(_currPos))
		{	
			_nextValue.setData(&number, sizeof(double));
			//_nextValue = _inputIterator->getItem();
		}*/	

		return _nextValue;
	}

	Value& IcWindowChunkIterator::getItem()
	{
		if (!_hasCurrent)
			throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
		return _nextValue;
	}

	Coordinates const& IcWindowChunkIterator::getPosition()
	{
		return _currPos;
	}

	bool IcWindowChunkIterator::setPosition(Coordinates const&pos)
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


	bool IcWindowChunkIterator::isEmpty()
	{
		return false;
	}

	void IcWindowChunkIterator::reset()
	{
		if (setPosition(_firstPos))
		{
			_hasCurrent = true;
			return;
		}
		++(*this);
	}

	void IcWindowChunkIterator::operator ++()
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

	bool IcWindowChunkIterator::end()
	{
		return !_hasCurrent;
	}

	ConstChunk const& IcWindowChunkIterator::getChunk()
	{
		return _chunk;
	}



//	* IcWindowChunk ------------------------------------------------------------------------
	IcWindowChunk::IcWindowChunk(IcWindowArray const& arr, AttributeID attr):
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

	Array const& IcWindowChunk::getArray() const
	{
		return _array;
	}


	const ArrayDesc& IcWindowChunk::getArrayDesc() const
	{
		return _array._desc;
	}

	const AttributeDesc& IcWindowChunk::getAttributeDesc() const
	{
		return _array._desc.getAttributes()[_attrID];
	}

	Coordinates const& IcWindowChunk::getFirstPosition(bool withOverlap) const
	{
		return _firstPos;
	}

	Coordinates const& IcWindowChunk::getLastPosition(bool withOVerlap) const
	{
		return _lastPos;
	}

	shared_ptr<ConstChunkIterator> IcWindowChunk::getConstIterator(int iterationMode) const
	{
		SCIDB_ASSERT( (_arrayIterator != NULL) );
		ConstChunk const& inputChunk = _arrayIterator->iterator->getChunk();	//chunk in SrcArr
		
		if (_array.getArrayDesc().getEmptyBitmapAttribute() && _attrID == _array.getArrayDesc().getEmptyBitmapAttribute()->getId())
		{
			return inputChunk.getConstIterator((iterationMode & ~ChunkIterator::INTENDED_TILE_MODE) | ChunkIterator::IGNORE_OVERLAPS);
		}	
		// materiliezed?

		return shared_ptr<ConstChunkIterator>(new IcWindowChunkIterator(*_arrayIterator, *this, iterationMode));
	}

	int IcWindowChunk:: getCompressionMethod() const
	{
		return _array._desc.getAttributes()[_attrID].getDefaultCompressionMethod();
	}


	void IcWindowChunk::setPosition(IcWindowArrayIterator const* iterator, Coordinates const& pos)
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







//	* IcWindowArrayIterator ---------------------------------------------------------------------


	IcWindowArrayIterator::IcWindowArrayIterator(IcWindowArray const& arr, AttributeID attrID, AttributeID input):
		array(arr),
		iterator(arr._inputArray->getConstIterator(input)),
		currPos(arr._dimensions.size()),
		chunk(arr, attrID)
	{
		reset();
	}
		
	ConstChunk const& IcWindowArrayIterator::getChunk()
	{
		if (!chunkInitialized)
		{
			chunk.setPosition(this, currPos);
			chunkInitialized = true;
		}
		return chunk;
	}

	Coordinates const& IcWindowArrayIterator::getPosition()
	{
		if (!hasCurrent)
			throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
		return currPos;
	}


	bool IcWindowArrayIterator::setPosition(Coordinates const& pos)
	{
		chunkInitialized = false;					//?
		if (!iterator->setPosition(pos))
		{
			return hasCurrent = false;
		}
		currPos = pos;
		return hasCurrent = true;
	}


	bool IcWindowArrayIterator::end()
	{
		return !hasCurrent;
	}

	void IcWindowArrayIterator::operator ++()
	{
		if (!hasCurrent)
			throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
		chunkInitialized = false;					//?
		++(*iterator);
		hasCurrent = !iterator->end();
		if (hasCurrent)
		{
			currPos = iterator->getPosition();
		}
	}

	void IcWindowArrayIterator::reset()
	{
		chunkInitialized = false;
		iterator->reset();
		hasCurrent = !iterator->end();
		if (hasCurrent)
		{
			currPos = iterator->getPosition();
		}
	}


//	* IcWindowArray     	---------------------------------------------------------------------

	IcWindowArray::IcWindowArray(ArrayDesc const& desc, shared_ptr<Array> const& inputArray,
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

	ArrayDesc const& IcWindowArray::getArrayDesc() const
	{
		return _desc;
	}

	shared_ptr<ConstArrayIterator> IcWindowArray::getConstIterator(AttributeID attr) const
	{
		if (_desc.getEmptyBitmapAttribute() && attr == _desc.getEmptyBitmapAttribute()->getId())
		{
			return shared_ptr<ConstArrayIterator>(new IcWindowArrayIterator(*this, attr, _inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId()));
		}
		return shared_ptr<ConstArrayIterator>(new IcWindowArrayIterator(*this, attr, _inputAttrIDs[attr] ));
	}


} // namespace

