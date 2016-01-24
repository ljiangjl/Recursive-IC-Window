


#include <vector>
#include <string>

#include "query/Aggregate.h"
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "query/FunctionDescription.h"
#include "query/Expression.h"
#include "query/Aggregate.h"


namespace scidb
{

using namespace std;
using namespace boost;

class IcWindowArray;
class IcWindowArrayIterator;
class MaterializedIcWindowChunkIterator;


struct WindowBoundaries
{
		WindowBoundaries()
				
		{
			_boundaries.first = _boundaries.second = 0;
		}

		WindowBoundaries(Coordinate first, Coordinate second)
		{
			SCIDB_ASSERT(first >= 0);
			SCIDB_ASSERT(second >= 0);

			_boundaries.first = first;
			_boundaries.second = second;

		}
		std::pair<Coordinate, Coordinate> _boundaries;
};

struct PriorityQueueNode {
	friend bool operator < (PriorityQueueNode n1, PriorityQueueNode n2)
	{
		return n1._value < n2._value;
	}
public:
	PriorityQueueNode(int pos, double value)
	{
		_value = value;
		_pos = pos;
	}
	double _value;
	int _pos;
};


class IcWindowChunk : public ConstChunk
{
	friend class IcWindowChunkIterator;
public:
	IcWindowChunk(IcWindowArray const& array, AttributeID attrID);

	virtual const ArrayDesc& getArrayDesc() const;
	virtual const AttributeDesc& getAttributeDesc() const;
	virtual Coordinates const& getFirstPosition(bool withOverlap) const;
	virtual Coordinates const& getLastPosition(bool withOverlap) const;
	virtual shared_ptr<ConstChunkIterator> getConstIterator(int iteratorMode) const;
	virtual int getCompressionMethod() const;
	virtual Array const& getArray() const;

	void setPosition(IcWindowArrayIterator const* iterator, Coordinates const& pos); 

private:

	IcWindowArray const& _array;
	IcWindowArrayIterator const* _arrayIterator;
	size_t _nDims;
	Coordinates _arrSize;
	Coordinates _firstPos;
	Coordinates _lastPos;
	AttributeID	_attrID;
	AggregatePtr _aggregate;

	Value _nextValue;
};


class IncrementalComputer
{
	friend class IcWindowChunkIterator;
public:
	void removeOld();
	void insertNew(int pos, double v);
	void baseWindowInitialize();
	double calculateCurrentValue(int pos);
	~IncrementalComputer()
	{
		delete []_interBuf;
	}
private:
	// sum | avg 
	int _aggrType;
	int _currPos;
	int _headPos;
	int _bufferSize;
	double _windowSize;
	double _sum;
	double* _interBuf;
	priority_queue<PriorityQueueNode> _heap;
	// max | min
};


class IcWindowChunkIterator : public ConstChunkIterator
{
public:
	virtual int getMode();
	virtual bool isEmpty();
	virtual Value& getItem();
	virtual void operator ++();
	virtual bool end();
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const& pos);
	virtual void reset();
	ConstChunk const& getChunk();

	IcWindowChunkIterator(IcWindowArrayIterator const& arrayIterator, IcWindowChunk const& aChunk, int mode);
private:
	Value& calculateNextValue();
	int getAggrTypeFromName(string const& name);
	void calculateWindowUnit(Coordinates const& first, Coordinates const& last, int d);
	void accumulate(Value const& v);
	

	IncrementalComputer _IcWorker;
	IcWindowArray const& _array;
	IcWindowChunk const& _chunk;
	Coordinates const& _firstPos;
	Coordinates const& _lastPos;
	Coordinates _currPos;
	bool _hasCurrent;
	AttributeID _attrID;
	AggregatePtr _aggregate;
	Value _defaultValue;
	int _iterationMode;
	shared_ptr<ConstChunkIterator> _inputIterator;
	shared_ptr<ConstArrayIterator> _emptyTagArrayIterator;
	shared_ptr<ConstChunkIterator> _emptyTagIterator;
	Value _nextValue;
	bool _noNullsCheck;
	double _result;	
	double number;
};



class IcWindowArrayIterator : public ConstArrayIterator
{
	friend class IcWindowChunkIterator;
	friend class IcWindowChunk;
public:
	virtual ConstChunk const& getChunk();
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const &pos);
	virtual bool end();
	virtual void operator ++();
	virtual void reset();

	IcWindowArrayIterator(IcWindowArray const& array, AttributeID id, AttributeID input);

private:
	IcWindowArray const& array;
	shared_ptr<ConstArrayIterator> iterator;	// iterator of the input array;
	Coordinates currPos;
	bool hasCurrent;
	IcWindowChunk chunk;
	bool chunkInitialized;
};



class IcWindowArray : public Array
{
	friend class IcWindowArrayIterator;
	friend class IcWindowChunkIterator;
	friend class IcWindowChunk;
public:
	virtual ArrayDesc const& getArrayDesc() const;
	virtual shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;
	
	IcWindowArray(ArrayDesc const& desc,
				  shared_ptr<Array> const& inputArray,
				  vector<WindowBoundaries> const& window,
				  vector<AttributeID> const& inputAttrIDs,
				  vector<AggregatePtr> const& aggregates);
private:
	ArrayDesc _desc;
	ArrayDesc _inputDesc;
	vector<WindowBoundaries> _window;
	Dimensions _dimensions;
	shared_ptr<Array> _inputArray;
	vector<AttributeID> _inputAttrIDs;
	vector<AggregatePtr> _aggregates;
};



}
