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
int tstart = 0;
int tend = inf;

struct myCmp
{
    bool operator() (const pair<int,int>& p1, const pair<int,int>& p2) const
    {
    	return p1.first < p2.first;
    }
};
bool myCmp2(const pair<int,int>& p1, const pair<int,int>& p2)
{
	return p1.first < p2.first;
}

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
	set<pair<int, int>, myCmp > s;
};


ibinstream & operator<<(ibinstream & m, const queryValue & v){
    m << v.arrivalTime;
    m << v.pn;
    //m << v.s;
    m << v.s.size();
    for (set<pair<int,int>, myCmp>::iterator it = v.s.begin(); it != v.s.end(); ++ it)
    	m << *it;
    return m;
}
obinstream & operator>>(obinstream & m, queryValue & v){
    m >> v.arrivalTime;
    m >> v.pn;
    //m >> v.s;
    size_t size;
    m >> size;
    pair<int,int> p;
    for (size_t i = 0; i < size; ++ i)
    {
    	m >> p;
    	v.s.insert(p);
    }
    return m;
}

//vertex
//KeyT, QValueT, NQValueT, MessageT, QueryT
class pVertex : public VertexOL<VertexID, queryValue, vertexValue, pair<int,int>, vector<int> >{
public:
	virtual queryValue init_value(vector<int>& query)
	{
		queryValue ret;
		ret.pn.resize(nqvalue().num_of_neighbors);
		for (int i = 0; i < nqvalue().num_of_neighbors; ++ i)
		{
			ret.pn[i] = nqvalue().v_time[i].size();
		}
		ret.arrivalTime = inf;
		//if (id == query[0]) ret.arrivalTime = query.size()==1?0:query[1];
		return ret;
	}

    virtual void compute(MessageContainer& messages)
    {
        if (superstep() == 1)
        {
            qvalue().s.clear();
            if (id == get_query()[1]) 
            {
                qvalue().arrivalTime = 0;
                for (int i = 0; i < nqvalue().neighbors.size(); ++ i)
                {
                    
                    vector<pair<int,int> >:: iterator it;
                    it = lower_bound(nqvalue().v_time[i].begin(), nqvalue().v_time[i].end(), make_pair(tstart,0), myCmp2);
                    while (it != nqvalue().v_time[i].end())
                    {
                    	send_message(nqvalue().neighbors[i], make_pair(it->first, it->first+it->second));
                    	it ++;
                    }
                    
                }
            }
            else
            {
                qvalue().arrivalTime = inf;
            }           
        }
        else
        {
            for (int i = 0; i < messages.size(); ++ i)
            {
            	pair<int,int>& p = messages[i];
            	set<pair<int,int> >::iterator it;
            	it = qvalue().s.lower_bound(p);
            	if (it == qvalue().s.end() || it->second > p.second)
            	{
            	    if (it == qvalue().s.end())
            	    {           	    
            	    	qvalue().s.insert(p);
            	    }
            	    else if (it->first == p.first)
            	    {
            	    	qvalue().s.erase(it);
            	    	qvalue().s.insert(p);
            	    }
            	    else 
            	    {
            	    	qvalue().s.insert(p);
            	    }
            	    
            	    //remove
            	    set<pair<int,int> >::iterator rp = qvalue().s.lower_bound(p);
            	    set<pair<int,int> >::iterator tmp;
            	    if (rp != qvalue().s.begin())
            	    {
            	    	rp--;
            	    	while(rp != qvalue().s.begin() && rp -> second >= p.second)
            	    	{
            	    	    tmp = rp; 
            	    	    rp--;
            	    	    qvalue().s.erase(tmp);
            	    	}
            	    	if (rp == qvalue().s.begin())
            	    	{
            	    	    if (rp->second >= p.second) qvalue().s.erase(rp);
            	    	}
            	    }
            	    
            	    if (p.second - p.first < qvalue().arrivalTime)
            	    qvalue().arrivalTime = p.second - p.first;
            	    
            	    //send
            	    for (int j = 0; j < nqvalue().neighbors.size(); ++ j)
            	    {
            	    	vector<pair<int,int> >::iterator it2;
            	    	it2 = lower_bound(nqvalue().v_time[j].begin(), nqvalue().v_time[j].end(), make_pair(p.second,0), myCmp2);
            	    	if (it2!=nqvalue().v_time[j].end())
            	    	send_message(nqvalue().neighbors[j], make_pair(p.first, it2->first+it2->second));
            	    	
            	    }
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

int main(int argc, char* argv[])
{
	in_path = argv[1];
	
	WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    pathWorker worker;
    //pathCombiner combiner;
    //worker.setCombiner(&combiner);
    
    worker.run(param);
    return 0;
}

