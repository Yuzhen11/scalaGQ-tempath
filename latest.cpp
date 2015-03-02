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
int src = 0;
int tstart = inf;
int tend = inf;


struct vertexValue
{
    int num_of_neighbors;
    vector<vector<pair<int,int> > > v_time; //(t,w)
    vector<int> neighbors;
};
ibinstream & operator<<(ibinstream & m, const vertexValue & v){
    m << v.num_of_neighbors;
    m << v.v_time;
    m << v.neighbors;
    return m;
}
obinstream & operator>>(obinstream & m, vertexValue & v){
    m >> v.num_of_neighbors;
    m >> v.v_time;
    m >> v.neighbors;
    return m;
}
struct queryValue
{
	int arrivalTime;
	vector<int> pn;
};
ibinstream & operator<<(ibinstream & m, const queryValue & v){
    m << v.arrivalTime;
    m << v.pn;
    return m;
}
obinstream & operator>>(obinstream & m, queryValue & v){
    m >> v.arrivalTime;
    m >> v.pn;
    return m;
}

//vertex
//KeyT, QValueT, NQValueT, MessageT, QueryT
class pVertex : public VertexOL<VertexID, queryValue, vertexValue, int, vector<int> >{
public:
	virtual queryValue init_value(vector<int>& query)
	{
		queryValue ret;
		ret.pn.resize(nqvalue().num_of_neighbors);
		for (int i = 0; i < nqvalue().num_of_neighbors; ++ i)
		{
			ret.pn[i] = 0;
		}
		ret.arrivalTime = -1;
		//if (id == query[0]) ret.arrivalTime = query.size()==1?0:query[1];
		return ret;
	}	
	
	void broadcast(int startTime)
    {
        for (int i = 0; i < nqvalue().num_of_neighbors; ++ i)
        {
            vector<pair<int, int> >:: iterator it; 
            pair<int,int> tmp(startTime, 0);
            vector<pair<int, int> >:: iterator vstart = nqvalue().v_time[i].begin()+qvalue().pn[i];
            it = upper_bound(vstart, nqvalue().v_time[i].end(), tmp);
            
            if (vstart != nqvalue().v_time[i].end())
            {
            	if (it->first == startTime) //not so cool 
            	{
            		send_message(nqvalue().neighbors[i], (*it).first-(*it).second);
	        	it++;
	        	qvalue().pn[i] = it - nqvalue().v_time[i].begin();
	        	//continue;
            	}
            	else if (it != nqvalue().v_time[i].begin() )
            	{
		    		it--;
					send_message(nqvalue().neighbors[i], (*it).first-(*it).second);
					it++;
					qvalue().pn[i] = it - nqvalue().v_time[i].begin();
				}
	    
	    	}

        }
    }
    virtual void compute(MessageContainer& messages)
    {
        if (superstep() == 1)
        {
            if (id == get_query()[1]) 
            {
                qvalue().arrivalTime = tstart;
                broadcast(tstart);
            }
            else
            {
                qvalue().arrivalTime = -1;
            }
        }
        else
        {
            int maxi = -1;
            for (int i = 0; i < messages.size(); ++ i)
            {
               if (messages[i] > maxi) maxi = messages[i];
            }
            if (maxi > qvalue().arrivalTime)
            {
                qvalue().arrivalTime = maxi;
                broadcast(maxi);
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
        int from, num_of_neighbors, to, num_of_t, w, t;
        ssin >> from >> num_of_neighbors;
        
        v->id = from;
        v->nqvalue().num_of_neighbors = num_of_neighbors;
        v->nqvalue().v_time.resize(num_of_neighbors);
        v->nqvalue().neighbors.resize(num_of_neighbors);
        
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
            //cout << i << endl;
            ssin >> to >> num_of_t >> w;
            v->nqvalue().neighbors[i] = to;
            for (int j = 0; j < num_of_t; ++ j)
            {
                ssin >> t;
                v->nqvalue().v_time[i].push_back(make_pair(t, w));
            }

        }
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
        int src = get_query()[1];
        int pos = get_vpos(src);
        if (pos != -1)
            activate(pos);
    }
    virtual void dump(pVertex* v, BufferedWriter& writer)
    {
    	//cout << v->id << " " << v->qvalue().arrivalTime << endl; 
        sprintf(buf, "%d %d\n", v->id, v->qvalue().arrivalTime);
        writer.write(buf);
    }

};

class pathCombiner : public Combiner<int>
{
public:
    virtual void combine(int & old, const int & new_msg)
    {
		if (old < new_msg) old = new_msg;
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

