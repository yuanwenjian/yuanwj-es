package com.yuanwj.util;

import com.yuanwj.model.User;
import net.sf.json.JSONObject;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by bmk on 17-7-13.
 */
public class ClientUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ClientUtil.class);

    private static TransportClient client;

    private static String ip = "127.0.0.1";

    private static String cluserName = "yuanwj";

    private static int port = 9300;

    public static TransportClient createClient() {
        try {
            if (client == null) {
                Settings settings = Settings.builder()
                        .put("cluster.name", cluserName)
                        .put("client.transport.sniff", true)
                        .put("client.transport.ping_timeout", "5s")
                        .put("client.transport.nodes_sampler_interval", "100s")
                        .build();
                client = new PreBuiltTransportClient(settings);

                client.addTransportAddress(new InetSocketTransportAddress(InetAddresses.forString(ip), port));
                System.out.println("连接成功");
                return client;
            }
        } catch (Exception e) {
            e.printStackTrace();
            client.close();
        }
        return client;
    }

    public static void createIndex(String indexName) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("index")
                    .startObject("analysis")
                    .startObject("analyzer")
                    .startObject("yuanwj").field("type", "custom").field("tokenizer", "my_token").field("filter", "my_pinyin").endObject()
                    .endObject()

                    .startObject("filter")
                    .startObject("my_pinyin").field("type", "pinyin").endObject()
                    .endObject()
                    .startObject("tokenizer")
                    .startObject("my_token").field("type", "ik_max_word").endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();

            XContentBuilder mappings = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("id").field("type", "integer").field("index", "not_analyzed").endObject()
                    .startObject("name").field("type", "text").field("analyzer", "yuanwj").field("search_analyzer", "ik_max_word").endObject()
                    .startObject("email")
                    .startObject("properties")
                    .startObject("keyword").field("type", "text").field("analyzer", "keyword").endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(builder.string());
            System.out.println(mappings.string());

            CreateIndexResponse response =
                    createClient().admin().indices().prepareCreate(indexName).addMapping("user", mappings).setSettings(builder).get();
            System.out.println(response.isAcknowledged());
        } catch (IOException e) {
            LOG.warn("创建索引出现异常");
            e.printStackTrace();
        }
    }

    public static void index() {
        String text = "Elasticsearch 依赖于索引和存储数据的 Lucene 后面的数据结构最适用于密集数据，比如，当所有文档都有相同的字段的时候。对于启用规范的字段（默认情况下为文本字段）或启用了文档值（默认情况下为数字，日期，IP和关键字）的情况尤其如此。\n" +
                "原因是 Lucene 内部标识了 documents（文档）所谓的 doc_id，它们是在 0 和 documents（文档）总数之间的一个整数。 这些 documents（文档）用于Lucene的内部API之间的通信：例如，使用 match 查询搜索 term 时会生成doc id的迭代器，然后使用这些 documents（文档）来检索规范的值以计算这些 documents（文档）的得分。实现此规范查找的方法是为每个 documents（文档）保留一个字节，然后通过读取索引中的 doc_id 字节来检索给定 documents（文档）的 id 的取值集合。虽然这是非常有效的，并且可以帮助 Lucene 快速访问每个 documents（文档）的标准值，但这样做的缺点是没有值的文档也需要一个字节的存储空间。\n" +
                "实际上，这意味着如果一个索引具有M个文档，则规范将需要每个字段M个于李怒阿字节的存储空间南，即使只出现在索引文档的一小部分中的字段也是如此。虽然文档取值稍微复杂一些，因为 doc值 有多种方式，但是可以根据字段的类型和字段存储的实际数据进行编码，问题是非常相似的。如果想了解 fielddata，在 Elasticsearch 2.0 之前的版本使用，之后替换成了 doc values，同样收到这个问题的影响，但是 fielddata 只是在内存占用上有影响，没有明确的具体化到磁盘。\n" +
                "请注意，尽管稀疏性最显着的影响是存储要求，但它也对索引速度和搜索速度有影响，因为这些字节对于没有字段的文档仍然需要在索引时写入并且在搜索时跳过。";
        Random random = new Random();
        StringBuffer sb = new StringBuffer("");
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 20; j++) {
                sb.append(text.charAt(random.nextInt(799)));
            }
            System.out.println(sb);
//            User user = new User();
            JSONObject user=new JSONObject();
            user.put("id",i);
            user.put("name",sb.toString() + "@biomarker.com.cn");
            JSONObject object=new JSONObject();
            object.put("keyword",sb.toString() + "@biomarker.com.cn");
            user.put("email",object);
            String json = user.toString();
            TransportClient client = createClient();
            IndexResponse response = client.prepareIndex("test", "user", String.valueOf(i)).setSource(json).get();
            sb = new StringBuffer("");
            System.out.println(response.toString());
        }
    }

    public static void search() {
        TransportClient client = createClient();
//        QueryStringQueryBuilder query = queryStringQuery("文具").field("name");
//        MatchQueryBuilder query=matchQuery("name","启用");
        TermQueryBuilder query = termQuery("email.keyword", " 况E空n度P影具t 规。可影t例索cc@biomarker.com.cn");
        SearchResponse response = client.prepareSearch("test").setTypes("user").setQuery(query).get();
        LOG.debug("查询完成,结果为{}", response.toString());

        System.out.println(response.toString());
    }

    public static void group() {
        TransportClient client = createClient();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("keyword").field("email.keyword");
        aggregation.size(100);
        SearchRequestBuilder searchRequest = client.prepareSearch().setIndices("test").setTypes("user").addAggregation(aggregation);
        SearchResponse response = searchRequest.execute().actionGet();
        Aggregations agg = response.getAggregations();
        Map<String,Aggregation> map=agg.getAsMap();
        StringTerms terms=(StringTerms) map.get("keyword");
        for (Terms.Bucket bucket:terms.getBuckets()){
            LOG.debug("分组结果为{}",bucket.getKeyAsString());
        }

        LOG.debug("查询完成,结果为{}", response.toString());
        System.out.println(response.toString());
    }

    public static void delete(String indexName) {
        TransportClient client = createClient();
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        client.admin().indices().delete(request).actionGet().isAcknowledged();
        LOG.debug("删除索引成功");

    }

    public static void main(String[] args) {
//        delete("test");
//        createIndex("test");
//        index();
        search();
//        group();
    }
}
