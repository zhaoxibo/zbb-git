package es.zbb_es;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class testEs {
     public static void main(String[] args) throws UnknownHostException {
    	// on startup bb 

    	 Settings settings = Settings.builder()
    		        .put("client.transport.sniff", true).build();
    	// Settings settings = Settings.builder()
    		        //.put("cluster.name", "myClusterName").build();
    	 
    	 TransportClient client = new PreBuiltTransportClient(settings)
    	         .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9300));
    	         //.addTransportAddress(new TransportAddress(InetAddress.getByName("host2"), 9300));

    	 // on shutdown
         System.out.println(client);
         
         String json = "{" +
        	        "\"user\":\"liuyueyue\"," +
        	        "\"postDate\":\"2013-01-30\"," +
        	        "\"message\":\"i amm liuyueyue \"" +
        	    "}";
         
         // 添加索引以及数据
         //add(client, json);
         //获取索引
         //getIndex(client);
         //delete API允许基于id从特定索引中删除一个类型化的JSON文档
         // delete(client);
         
         //通过查询API的删除可以根据查询结果删除给定的一组文档
         //deleteByQuery(client);
         //由于这是一个长时间运行的操作，如果您希望异步执行，可以调用execute而不是get，并提供如下侦听器
         //deleteByQuery1(client);
         /**
         try {
            //根据条件更新索引
			UpdateRequest(client);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}*/
         
         
         
         
             
         
    	 client.close();
	}	
     //利用elasticsearch 的工具类构建json
     public static String createJson(String name,Date postDate,String message) {
    	 XContentBuilder builder;
    	 String json = null;
		try {
			builder = jsonBuilder()
					    .startObject()
					        .field("user",name)
					        .field("postDate",postDate)
					        .field("message",message)
					    .endObject();
	         json = Strings.toString(builder);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	 return json;
     } 
     public static void add(TransportClient client,String json) {
     	IndexResponse response = client.prepareIndex("twitter", "tweet")//(参数1索引名称,参数2索引类型)
    	        .setSource(json, XContentType.JSON)
    	        .get();
     	//在es中的结果为
    	/**
    	 {
    		"_index": "twitter",
    		"_type": "tweet",
    		"_id": "u7MZ42QBJNQWlxh2HF5F",
    		"_version": 1,
    		"_score": 1,
    		"_source": {
    		"user": "zbb",
    		"postDate": "2013-01-30",
    		"message": "i amm zbb "
    		}
    	}
    	 * 
    	 */
     // Index name
     	String _index = response.getIndex();
     	// Type name
     	String _type = response.getType();
     	// Document ID (generated or not)
     	String _id = response.getId();
     	// Version (if it's the first time you index this document, you will get: 1)
     	long _version = response.getVersion();
     	// status has stored current instance statement.
     	RestStatus status = response.status();
     	System.out.println("_index:"+_index);
     	System.out.println("_type:"+_type);
     	System.out.println("_id:"+_id);
     	System.out.println("_version:"+_version);
     	System.out.println(status);
     }
     public static void getIndex(TransportClient client) {
    	 GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
    	 System.out.println(response);
     }
     public static void delete (TransportClient client) {
    	 DeleteResponse response = client.prepareDelete("twitter1", "tweet", "1").get();
    	 System.out.println(response.getId());
     }
     public static void deleteByQuery(TransportClient client) {
    	 BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
    			    .filter(QueryBuilders.matchQuery("user","zbb")) //条件user=zbb
    			    .source("twitter") //index名称                                 
    			    .get();                                             
    			long deleted = response.getDeleted();  
    			System.out.println("删除了 "+deleted+" 条数据");
     }
     public static void deleteByQuery1(TransportClient client) {
    	 DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
    	    .filter(QueryBuilders.matchQuery("user","liuyueyue"))  //条件user=liuyueyue  
    	    .source("twitter1")  //index名称                                    
    	    .execute(new ActionListener<BulkByScrollResponse>() {   
    	        public void onResponse(BulkByScrollResponse response) {
    	            long deleted = response.getDeleted(); 
    	            System.out.println("删除了 "+deleted+" 条数据");
    	        }
    	        public void onFailure(Exception e) {
    	        	System.out.println("出错啦!");
    	        }
    	    });
     }
     public static void UpdateRequest(TransportClient client) throws IOException, InterruptedException, ExecutionException {
    	 // 条件
    	 IndexRequest indexRequest = new IndexRequest("index", "type", "1")// 参数说明 (_index=index,_type=type,_id=1)
    		        .source(jsonBuilder()
    		            .startObject()
    		                .field("user","zjj")  //user=zjj
    		                .field("message","i amm zjj ")   //message=i amm zjj 
    		            .endObject());
    	 
    	 UpdateRequest updateRequest = new UpdateRequest("index", "type", "1")
    		        .doc(jsonBuilder()
    		            .startObject()
    		                .field("message", "我是zjj")
    		                .field("postDate",new Date())
    		            .endObject())
    		        .upsert(indexRequest); 
    	 
    	 org.elasticsearch.action.update.UpdateResponse res=client.update(updateRequest).get();
    	 //  如果没有符合条件的记录 elasticsearch  会给你插入一条进去
    	 System.out.println(res);
     }
}
