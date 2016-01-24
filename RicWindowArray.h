
#include <vector>
#include <string>

#include "query/Aggregate.h"
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "query/FunctionDescription.h"
#include "query/Expression.h"
#include "query/Aggregate.h"
#include "BufferTool.h"


namespace scidb
{

using namespace std;
using namespace boost;

class RicWindowArray;
class RicWindowArrayIterator;
class MaterializedRicWindowChunkIterator;




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


class RicWindowChunk : public ConstChunk
{
	friend class RicWindowChunkIterator;
public:
	RicWindowChunk(RicWindowArray const& array, AttributeID attrID);

	virtual const ArrayDesc& getArrayDesc() const;
	virtual const AttributeDesc& getAttributeDesc() const;
	virtual Coordinates const& getFirstPosition(bool withOverlap) const;
	virtual Coordinates const& getLastPosition(bool withOverlap) const;
	virtual shared_ptr<ConstChunkIterator> getConstIterator(int iteratorMode) const;
	virtual int getCompressionMethod() const;
	virtual Array const& getArray() const;

	void setPosition(RicWindowArrayIterator const* iterator, Coordinates const& pos); 

private:

	RicWindowArray const& _array;
	RicWindowArrayIterator const* _arrayIterator;
	size_t _nDims;
	Coordinates _arrSize;
	Coordinates _firstPos;
	Coordinates _lastPos;
	AttributeID	_attrID;
	AggregatePtr _aggregate;

	Value _nextValue;
};



class RicWindowChunkIterator : public ConstChunkIterator
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

	RicWindowChunkIterator(RicWindowArrayIterator const& arrayIterator, RicWindowChunk const& aChunk, int mode);
	~RicWindowChunkIterator();
private:
	Value& calculateNextValue();
	aggrType getAggrType(string const& name);
	void initializeBufferTools();
	BufferTool* createSpecificBuffer(size_t size);
	void releaseSpecificBuffer(BufferTool* ptr);
	void calculateWindowUnit(Coordinates const& first, Coordinates const& last, int d);
	void accumulate(Value const& v);	
	double accessValue(Coordinates coor);
	void prepare(size_t dim, int start, int end);
	void recursiveUpdate(double v);


	//IncrementalComputer _RicWorker;
	// buffer tool set
	vector < vector < BufferTool*> > _bufferSet;
	RicWindowArrayIterator const& _arrayIter;
	RicWindowArray const& _array;
	RicWindowChunk const& _chunk;
	Coordinates const& _firstPos;
	Coordinates const& _lastPos;
	size_t _nDims;
	vector<WindowBoundaries> _window;
	Coordinates _currPos;
	Coordinates _movePos;
	Coordinates _chunkSize;
	Coordinates _windowSize;
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
	aggrType _aggrType;
};



class RicWindowArrayIterator : public ConstArrayIterator
{
	friend class RicWindowChunkIterator;
	friend class RicWindowChunk;
public:
	virtual ConstChunk const& getChunk();
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const &pos);
	virtual bool end();
	virtual void operator ++();
	virtual void reset();

	RicWindowArrayIterator(RicWindowArray const& array, AttributeID id, AttributeID input);

	double number;

private:
	RicWindowArray const& array;
	shared_ptr<ConstArrayIterator> iterator;	// iterator of the input array;
	Coordinates currPos;
	bool hasCurrent;
	RicWindowChunk chunk;
	bool chunkInitialized;
};



class RicWindowArray : public Array
{
	friend class RicWindowArrayIterator;
	friend class RicWindowChunkIterator;
	friend class RicWindowChunk;
public:
	virtual ArrayDesc const& getArrayDesc() const;
	virtual shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;
	
	RicWindowArray(ArrayDesc const& desc,
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
