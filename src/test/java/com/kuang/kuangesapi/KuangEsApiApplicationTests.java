package com.kuang.kuangesapi;

import com.alibaba.fastjson.JSON;
import com.kuang.kuangesapi.pojo.User;
import net.minidev.json.JSONArray;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class KuangEsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    //测试索引的创建
    @Test
    void testCreateIndex() throws IOException {
        //1.创建索引的请求
        CreateIndexRequest request = new CreateIndexRequest("jiaqi_index");
        //2客户端执行请求，请求后获得响应
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response);//org.elasticsearch.client.indices.CreateIndexResponse@3bbdaf8c
    }

    //测试索引是否存在
    @Test
    void testExistIndex() throws IOException {
        //1.创建索引请求
        GetIndexRequest request = new GetIndexRequest("jiaqi_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists); //true
    }


    //删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("jiaqi_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("删除索引--------" + delete.isAcknowledged());//删除索引--------true
    }

    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = new User("狂神说", 3);
        IndexRequest request = new IndexRequest("jiaqi_index");
        request.id("1");
        //设置超时时间
        request.timeout("1s");
        //将数据放到json字符串
        IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);
        //发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println("添加文档-------" + response.toString());
        System.out.println("添加文档-------" + response.status());//对应我们命令返回的状态CREATED
        //结果
        //添加文档-------IndexResponse[index=jiaqi_index,type=_doc,id=1,version=1,result=created,seqNo=0,primaryTerm=1,shards={"total":2,"successful":1,"failed":0}]
        //添加文档-------CREATED
    }

    //测试文档是否存在
    @Test
    void testExistDocument() throws IOException {
        //测试文档的 没有index
        GetRequest request = new GetRequest("jiaqi_index", "1");
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        //没有indices()了
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println("测试文档是否存在-----" + exists);
    }

    //测试获取文档
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("jiaqi_index", "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println("测试获取文档-----" + response.getSource());
        System.out.println("测试获取文档-----" + response.getSourceAsString());
        System.out.println("测试获取文档-----" + response);
        //结果
//        测试获取文档-----{"age":27,"name":"lisen"}
//        测试获取文档-----{"_index":"lisen_index","_type":"_doc","_id":"1","_version":1,"_seq_no":0,"_primary_term":1,"found":true,"_source":{"age":27,"name":"lisen"}}
    }

    //测试修改文档
    @Test
    void testUpdateDocument() throws IOException {
        User user = new User("刘家岐", 18);
        //修改是id为1的
        UpdateRequest request = new UpdateRequest("jiaqi_index", "1");
        request.timeout("1s");
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        System.out.println("测试修改文档-----" + updateResponse);
        System.out.println("测试修改文档-----" + updateResponse.status());
        //测试修改文档-----UpdateResponse[index=jiaqi_index,type=_doc,id=1,version=2,seqNo=1,primaryTerm=1,result=updated,shards=ShardInfo{total=2, successful=1, failures=[]}]
        //测试修改文档-----OK
    }

    //测试删除文档
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("jiaqi_index", "1");
        request.timeout("1s");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println("测试删除文档------" + response.status());
        //测试删除文档------OK
    }

    //测试批量添加文档
    @Test
    void testBulkAddDocument() throws IOException {
        ArrayList<User> userlist = new ArrayList<User>();
        userlist.add(new User("ljq1", 5));
        userlist.add(new User("ljq2", 6));
        userlist.add(new User("ljq3", 40));
        userlist.add(new User("ljq4", 25));
        userlist.add(new User("ljq5", 15));
        userlist.add(new User("ljq6", 35));

        //批量操作的Request
        BulkRequest request = new BulkRequest();
        request.timeout("10s");

        for (int i = 0; i < userlist.size(); i++) {
            request.add(
                    new IndexRequest("jiaqi_index")
                            .id("" + (i + 1))
                            .source(JSON.toJSONString(userlist.get(i)), XContentType.JSON));
        }

        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        //response.hasFailures()是否是失败的
        System.out.println("测试批量添加文档-----" + response.hasFailures());
        //结果:false为成功 true为失败
        //测试批量添加文档-----false
    }


    //测试查询文档
    @Test
    void testSearchDocument() throws IOException {
        SearchRequest request = new SearchRequest("jiaqi_index");
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //设置高亮
        sourceBuilder.highlighter();

        //term name为ljq1的
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "ljq1");
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);
        //分页
//        sourceBuilder.from(0);
//        sourceBuilder.size(2);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //放入条件
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        System.out.println("测试查询文档-----" + JSON.toJSONString(response.getHits()));
        System.out.println("=====================");
        for (SearchHit documentFields : response.getHits().getHits()) {
            System.out.println("测试查询文档--遍历参数--" + documentFields.getSourceAsMap());
        }
        //测试查询文档-----{"fragment":true,"hits":[{"fields":{},"fragment":false,"highlightFields":{},"id":"1","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":1.8413742,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"cyx1","age":5},"sourceAsString":"{\"age\":5,\"name\":\"cyx1\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1}],"maxScore":1.8413742,"totalHits":{"relation":"EQUAL_TO","value":1}}
        //=====================
        //测试查询文档--遍历参数--{name=cyx1, age=5}
    }

}
