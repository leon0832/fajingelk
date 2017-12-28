package com.fajing.elastic;

import com.fajing.model.BasicFile;
import com.fajing.service.BasicFileService;
import com.jfinal.kit.PropKit;
import com.util.JFinalModelCase;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticService extends JFinalModelCase {

	private static BasicFileService basicFileService = new BasicFileService();

	private Settings settings = Settings.settingsBuilder().build();

	private TransportClient client;

	//es index 相当于数据库名
	private String indexName = PropKit.get("es_index_name");

	//相当于表名
	private String indexType = PropKit.get("es_index_type");

	public ElasticService() {
		try {
			client = TransportClient.builder()
					.settings(settings)
					.build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(PropKit.get("es_host")),
							Integer.parseInt(PropKit.get("es_port"))));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}


	/**
	 * 第一步创建mapping,设计数据表结构
	 *
	 * @return
	 */
	private XContentBuilder createMapping() {
		XContentBuilder mapping = null;
		try {
			mapping = XContentFactory.jsonBuilder()
					.startObject().startObject(indexType).startObject("properties")

					.startObject("id").field("type", "long").endObject()
					.startObject("creation_time").field("type", "string").endObject()
					.startObject("pid").field("type", "long").endObject()
					.startObject("pname").field("type", "string").field("analyzer", "ik_smart").endObject()
					.startObject("content").field("type", "string").field("analyzer", "ik_smart").endObject()
					.startObject("remark").field("type", "string").field("analyzer", "ik_smart").endObject()
					.startObject("sequence").field("type", "string").field("analyzer", "ik_smart").endObject()
					.startObject("sequence_number").field("type", "integer").endObject()
					.startObject("basic_static").field("type", "integer").endObject()

					.endObject().endObject().endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return mapping;
	}

	/**
	 * 创建索引
	 */
	private void createIndex() {
		XContentBuilder mapping = createMapping();

		//建立数据库，数据表，数据结构
		client.admin().indices().prepareCreate(indexName).execute().actionGet();

		PutMappingRequest putMappingRequest = Requests.putMappingRequest(indexName).type(indexType).source(mapping);

		PutMappingResponse putMappingResponse = client.admin().indices().putMapping(putMappingRequest).actionGet();

		if (!putMappingResponse.isAcknowledged()) {
			System.out.println("无法创建mapping");
		} else {
			System.out.println("创建mapping成功");
		}
	}


	/**
	 * 向索引添加数据
	 *
	 * @return
	 */
	private Integer addDataToIndex() {

		List<BasicFile> list = basicFileService.queryAllList();

		List<String> basicFileList = new ArrayList<>();

		for (BasicFile basicFile :
				list) {
			basicFileList.add(objToJson(basicFile));
		}


		//创建索引
		List<IndexRequest> requests = new ArrayList<IndexRequest>();
		for (String str : basicFileList) {
			IndexRequest request = client.prepareIndex(indexName, indexType)
					.setSource(str).request();
			requests.add(request);
		}

		//批量创建索引
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (IndexRequest request : requests) {
			bulkRequestBuilder.add(request);
		}

		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();

		if (bulkResponse.hasFailures()) {
			System.out.println("创建索引出错");
		}
		return bulkRequestBuilder.numberOfActions();
	}

	/**
	 * 将对象转为json字符串
	 *
	 * @param basicFile
	 * @return
	 */
	private String objToJson(BasicFile basicFile) {
		String jsonStr = null;

		try {
			XContentBuilder jsonBuild = XContentFactory.jsonBuilder();

			jsonBuild.startObject()
					.field("id", basicFile.getId())
					.field("creation_time", basicFile.getCreationTime())
					.field("pid", basicFile.getPid())
					.field("pname", basicFile.getPname())
					.field("content", basicFile.getContent())
					.field("remark", basicFile.getRemark())
					.field("sequence", basicFile.getSequence())
					.field("sequence_number", basicFile.getSequenceNumber())
					.field("basic_static", basicFile.getBasicStatic())
					.endObject();

			jsonStr = jsonBuild.string();
			System.out.println(jsonStr);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return jsonStr;
	}

	/**
	 * search elasticsearch
	 *
	 * @param str
	 * @param pid
	 * @param from
	 * @param size
	 * @return
	 */
	private List<BasicFile> searchFaJing(String str, Integer pid, Integer from, Integer size) {

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setTypes(indexType);

		//设置分页
		searchRequestBuilder.setFrom(from).setSize(size);

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		if (pid != null) {
			boolQueryBuilder.filter(QueryBuilders.termQuery("pid", pid));
		}

		//content/sequence/remark
		QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(str, "content", "sequence", "remark")
				.analyzer("ik_smart")
				.type("best_fields").tieBreaker(0.3f);
		if (str != null && !"".equals(str)) {
			boolQueryBuilder.filter(queryBuilder);
		}
		searchRequestBuilder.setQuery(boolQueryBuilder);

		searchRequestBuilder.addHighlightedField("sequence");
		searchRequestBuilder.addHighlightedField("content");
		searchRequestBuilder.addHighlightedField("remark");
//		searchRequestBuilder.setHighlighterPreTags("<span style=\"color:red\">");
//		searchRequestBuilder.setHighlighterPostTags("</span>");
		searchRequestBuilder.setHighlighterPreTags("<em>");
		searchRequestBuilder.setHighlighterPostTags("</em>");

//
		//开始搜索
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
		//一般的搜索引擎中，高亮
		SearchHits searchHits = searchResponse.getHits();
		SearchHit[] hits = searchHits.getHits();

		List<BasicFile> listNewFile = new ArrayList<>();

		for (SearchHit hit :
				hits) {

			Map<String, Object> map = hit.getSource();
			BasicFile basic = new BasicFile();
			basic.setId(Long.parseLong(map.get("id").toString()));
			System.out.println(map.get("creation_time").toString());
//			basic.setCreationTime(Timestamp.valueOf(map.get("creation_time").toString()));
			basic.setPid(Long.parseLong(map.get("pid").toString()));
			basic.setPname(map.get("pname").toString());
			basic.setContent(map.get("content").toString());
			basic.setRemark(map.get("remark").toString());
			basic.setSequence(map.get("sequence").toString());
			basic.setSequenceNumber(Integer.parseInt(map.get("sequence_number").toString()));

			Map<String, HighlightField> result = hit.highlightFields();

			// 从设定的高亮域中取得指定域
			HighlightField contentField = result.get("content");
			if (contentField != null) {
				// 取得定义的高亮标签
				Text[] contentTexts = contentField.fragments();
				// 为title串值增加自定义的高亮标签
				StringBuffer content = new StringBuffer();
				for (Text text : contentTexts) {
					content.append(text);
				}
				// 将追加了高亮标签的串值重新填充到对应的对象
				basic.setContent(content.toString());
				System.out.println(content);
			}

			// 从设定的高亮域中取得指定域
			HighlightField remarkField = result.get("remark");
			if (remarkField != null) {
				// 取得定义的高亮标签
				Text[] remarkTexts = remarkField.fragments();
				// 为remark串值增加自定义的高亮标签
				StringBuffer remark = new StringBuffer();
				for (Text text : remarkTexts) {
					remark.append(text);
				}
				// 将追加了高亮标签的串值重新填充到对应的对象
				basic.setRemark(remark.toString());
				System.out.println(remark.toString());
			}

			// 从设定的高亮域中取得指定域
			HighlightField sequenceField = result.get("sequence");
			if (sequenceField != null) {
				// 取得定义的高亮标签
				Text[] sequenceTexts = sequenceField.fragments();
				// 为sequence串值增加自定义的高亮标签
				StringBuffer sequence = new StringBuffer();
				for (Text text : sequenceTexts) {
					sequence.append(text);
				}
				basic.setSequence(sequence.toString());
				System.out.println(sequence.toString());
			}

			if (str == null || "".equals(str)) {
				basic.setPname("<em>" + basic.getPname() + "</em>");
			}

			listNewFile.add(basic);
		}
		//耗时
		Long useTime = searchResponse.getTookInMillis();
		System.out.println(useTime);
		System.out.println(hits.length);
		return listNewFile;
	}


	@Test
	public void TestDB() {
		searchFaJing("中华人民共和国国旗",6,0,8);

		//mysql to es
//		ElasticService es = new ElasticService();
//		es.createIndex();
//		Integer count=es.addDataToIndex();
//		System.out.println(count);
	}

}
