#ifndef BUFFERTOOL_H
#define BUFFERTOOL_H

#define NULL_VALUE -201420723.327024102
#define MAX_VALUE 1e100
#define MIN_VALUE -1e100


enum aggrType { SUM, AVG, MIN, MAX, VAR, STDEV};

class BufferTool {
public:

	BufferTool() {}

	BufferTool(int l) {
		len = l;
		buffer = new double[len];
		head = 0;
	}
	 
	~BufferTool() {
		delete [] buffer;
		if (clearType == AVG) {
			delete [] countBuf;
		} else if (clearType == VAR || clearType == STDEV) {
			delete [] countBuf;
			delete [] sum2Buf;
		} else if (clearType == MIN || clearType == MAX) {
		}
	}

	virtual void remove() {};
	//virtual void insert(double) {};
	virtual void insert(double, size_t) {};
	virtual void insert(double, size_t, double) {};
	virtual void clear() {};
	virtual double getCurrent() { return 0; }
	virtual double getSum2() { return 0.0; }
	size_t getNum() { return num; }
	
protected:
	int len;
	int head;
	double* buffer;
	double* sum2Buf;
	size_t* countBuf;
	size_t num;
	aggrType clearType;			// to show which ones of the buffers are allocated so that they needed to be released in the destructor
};


class SumBuffer : public BufferTool {

public:
	SumBuffer(int l) : BufferTool(l) { 
		sum = 0;
		clearType = SUM;
	}

	void remove() {
		sum -= buffer[head];
		buffer[head] = 0;
	}

	void insert(double v, size_t n) {
		if (v == NULL_VALUE)
			v = 0;
		sum += v;
		buffer[head] = v;
		head++;
		if (head == len) head = 0;
	}

	void clear() {
		sum = 0;
		head = 0;
	}

	double getCurrent() { return sum; }

private:
	double sum;
};

class AvgBuffer : public BufferTool {

public:
	AvgBuffer(int l) : BufferTool(l) {
		countBuf = new size_t[l];
		num = 0;
		sum = 0;
		clearType = AVG;			// to show that countBuf is also needed to be released when destruct
	}

	void remove() {
		sum -= buffer[head];
		num -= countBuf[head]; 
		buffer[head] = 0;
		countBuf[head] = 0;
	}

	void insert(double v, size_t n) {
		if ( v == NULL_VALUE) {
			v = 0;
			n = 0;
		}
		double sv = v * n;
		sum += sv;
		buffer[head] = sv;
		num += n;
		countBuf[head] = n;
		
		head++;
		if (head == len) head = 0;
	}

	void clear() {
		sum = 0;
		num = 0;
		head = 0;
	}

	double getCurrent() { 
		if (num > 0 )
			return sum/num;
		else
			return 0;
	}
private:
	double sum;
};



class VarBuffer : public BufferTool {
public:
	VarBuffer(int l) : BufferTool(l) {
		countBuf = new size_t[l];
		sum2Buf = new double[l];
		sum = sum2 = num = 0;
		clearType = VAR;		// countBuf and sum2Buf is also needed to be released when destruct
	}

	void remove() {
		sum -= buffer[head];
		buffer[head] = 0;
		sum2 -= sum2Buf[head];
		sum2Buf[head] = 0;
		num -= countBuf[head];
		countBuf[head] = 0;
	}


	void insert(double v, size_t n, double s) {
		double s2;
		if (v == NULL_VALUE) {
			n = 0;
			s = 0;
			s2 = 0;
		} else {
			s2 = v;
		}
		sum += s;
		buffer[head] = s;
		num += n;
		countBuf[head] = n;
		sum2 += s2;
		sum2Buf[head] = s2;

		head++;
		if (head == len) head = 0;	
	}

	void clear() {
		sum = 0;
		sum2 = 0;
		num = 0;
		head = 0;
	}

	double getCurrent() {
		return sum;
		/*
		if (num > 1) { 
			return (sum2 - sum*sum/num)/(num-1);
		} else if (num == 1) {
			return 0;
		}
		else {
			return -1;
		}
		*/
	}


	double getSum2() {
		return sum2;
	}
private: 
	double sum;
	double sum2;
};




class MinQueue : public BufferTool {
public:
	MinQueue(int l) : BufferTool(l) {
		pos = new int[len];
		clear();
	}

	void remove() {
		leftPos++;
		while (num > 0 && pos[head] < leftPos) {
			head++;
			if (head == len) head = 0;
			num--;
		}
	}

	void insert(double v, size_t n) {
		if (v == NULL_VALUE) {
			v = MAX_VALUE;
		}
		curPos++;
		while (num > 0 && v < buffer[tail]) {
			tail--;
			if (tail < 0) tail = len - 1;
			num--;
		}
		tail++;
		if (tail == len) tail = 0;
		buffer[tail] = v;
		pos[tail] = curPos;
		num++;
	}

	void clear() {
		num = 0;
		head = 0;
		tail = -1;
		leftPos = 0;
		curPos = -1;
	}

	double getCurrent() { return buffer[head]; }

private:
	int * pos;
	int tail;
	int leftPos, curPos;
	int num;
};



class MaxQueue : public BufferTool {
public:
	MaxQueue(int l) : BufferTool(l) {
		pos = new int[len];
	}

	void remove() {
		leftPos++;
		while (num > 0 && pos[head] < leftPos) {
			head++;
			if (head == len) head = 0;
			num--;
		}
	}

	void insert(double v, size_t n) {
		if (v == NULL_VALUE)
			v = MIN_VALUE;
		curPos++;
		while (num > 0 && v > buffer[tail]) {
			tail--;
			if (tail < 0) tail = len - 1;
			num--;
		}
		tail++;
		if (tail == len) tail = 0;
		buffer[tail] = v;
		pos[tail] = curPos;
		num++;
	}

	void clear() {
		num = 0;
		head = 0;
		tail = -1;
		leftPos = 0;
		curPos = -1;
	}

	double getCurrent() { return buffer[head]; }

private:
	int * pos;
	int tail;
	int leftPos, curPos;
	int num;
};



#endif
