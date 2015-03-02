#include "ol/pregel-ol-dev.h"
#include "utils/type.h"
#include <sstream>
#include <set>
#include <map>
#include <algorithm>
#include <iostream>
using namespace std;

string in_path = "/yuzhen/toyP";
string out_path = "/yuzhen/output";
bool use_combiner = true;


const int inf = 1e9;
//int src = 0;
int tstart = 0;
int tend = inf;

struct vertexValue
{

    int originalID;
    int timestamp;
    vector<pair<int,int> > neighbors;
    int toOriginalId;
};
ibinstream & operator<<(ibinstream & m, const vertexValue & v){

    m << v.originalID;
    m << v.timestamp;
    m << v.neighbors;
    m << v.toOriginalId;
    return m;
}
obinstream & operator>>(obinstream & m, vertexValue & v){

    m >> v.originalID;
    m >> v.timestamp;
    m >> v.neighbors;
    m >> v.toOriginalId;
    return m;
}
struct queryValue
{
	int arrivalTime;
	int vis;
};
ibinstream & operator<<(ibinstream & m, const queryValue & v){

    m << v.arrivalTime;
    m << v.vis;
    return m;
}
obinstream & operator>>(obinstream & m, queryValue & v){

    m >> v.arrivalTime;
    m >> v.vis;
    return m;
}
//vertex
//KeyT, QValueT, NQValueT, MessageT, QueryT
class pVertex : public VertexOL<VertexID, queryValue, vertexValue, int, vector<int> >{

public:
	virtual queryValue init_value(vector<int>& query)
	{
		queryValue ret;
		ret.arrivalTime = inf;
		ret.vis = -1;
		//if (id == query[0]) ret.arrivalTime = query.size()==1?0:query[1];
		return ret;
	}
    void broadcast()
    {
    	for (int i = 0; i < nqvalue().neighbors.size(); ++ i) 
           send_message(nqvalue().neighbors[i].first, qvalue().arrivalTime + nqvalue().neighbors[i].second);
    }
    virtual void compute(MessageContainer& messages)
    {
		if (superstep() == 1)
		{
			qvalue().arrivalTime = inf;
			if (nqvalue().originalID == get_query()[1]) 
			{
				qvalue().arrivalTime = 0;
				send_message(nqvalue().toOriginalId, qvalue().arrivalTime);
		   		broadcast();
			}
		   
		}
		else
		{
			if (nqvalue().timestamp < 0) //virtual vertex
			{
				int mini = inf;
		        for (int i = 0; i < messages.size(); ++ i) 
		        {
		        	if (messages[i] < mini) mini = messages[i];
		        }
		        qvalue().arrivalTime = mini;
			}
			else
			{
				int mini = inf;
				for (int i = 0; i < messages.size(); ++ i) 
				{
					if (messages[i] < mini) mini = messages[i];
				}
				if (mini < qvalue().arrivalTime)
				{
					qvalue().arrivalTime = mini;
					send_message(nqvalue().toOriginalId, qvalue().arrivalTime);
					broadcast();
				}
			}
		}
		vote_to_halt();
    }
};

class pathWorker : public WorkerOL_auto<pVertex>{
    char buf[1000];
public:
    virtual pVertex* toVertex(char * line)
    {
        pVertex* v = new pVertex;
        istringstream ssin(line);
        int from, o_id, timestamp, num_of_neighbors, to, w;
        //format: from originalID timestamp num_of_neighbors n1.id n1.w ... toOriginalId;
        //timestamp == -1 means virtual node 
        ssin >> from >> o_id >> timestamp >> num_of_neighbors;
        
        v->id = from;
        v->nqvalue().originalID = o_id;
        v->nqvalue().timestamp = timestamp;
        v->nqvalue().neighbors.resize(num_of_neighbors);
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
            ssin >> to >> w;
            v->nqvalue().neighbors[i] = make_pair(to, w);
        }
        ssin >> v->nqvalue().toOriginalId;
        return v;
        //cout << "load done" << endl;
    }
    virtual vector<int> toQuery(char* line)
    {
        //query type is vector<int> 
        vector<int> ret;
        istringstream ssin(line);
        int tmp;
        while(ssin >> tmp) ret.push_back(tmp);
        
        return ret;
    }
    virtual void init(VertexContainer& vertex_vec)
    {
        for (int i = 0; i < vertex_vec.size(); ++ i)
        {
        	if (vertex_vec[i]->nqvalue().originalID == get_query()[1]
        	&& vertex_vec[i]->nqvalue().timestamp >= 0)
        		activate(i);
        }
    }
    virtual void dump(pVertex* v, BufferedWriter& writer)
    {
        if (v->nqvalue().timestamp < 0)
    	{
			//cout << v->nqvalue().originalID << " " << v->qvalue().arrivalTime << endl; 
		    sprintf(buf, "%d %d\n", v->nqvalue().originalID, v->qvalue().arrivalTime);
		    writer.write(buf);
        }
    }
};
class pathCombiner : public Combiner<int>
{
public:
    virtual void combine(int & old, const int & new_msg)
    {
		if (old > new_msg) old = new_msg;
    }
};

int main(int argc, char* argv[])
{
	in_path = argv[1];
	
	WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    pathWorker worker;
    pathCombiner combiner;
    worker.setCombiner(&combiner);
    
    worker.run(param);
    return 0;
}

