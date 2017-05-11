package org.xululabs.twittertool_v2;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.sort.SortOrder;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.TwitterException;

public  class DeployServer extends AbstractVerticle {

	HttpServer server;
	Router router;
	Twitter4jApi twitter4jApi;
	String host;
	int port;
	ElasticsearchApi elasticsearch;
	static RoutingContext routingContext;
	String esHost;
	String esIndex;
	String Index;
	int esPort;
	int documentsSize;
	int bulkSize = 1000;

	/**
	 * constructor use to initialize values
	 */
	 public  DeployServer()  {
			this.host = "localhost";
			this.port = 8182;
			this.twitter4jApi = new Twitter4jApi();
			this.elasticsearch = new ElasticsearchApi();
			this.esHost = "localhost";
			this.esPort = 9300;
			this.esIndex = "twitter";
			this.Index = "user";
			this.documentsSize = 1000;
		
	}

	/**
	 * Deploying the verical
	 */
	@Override
	public void start() {	

		server = vertx.createHttpServer();
		router = Router.router(vertx);
		// Enable multipart form data parsing
		router.route().handler(BodyHandler.create());
		router.route().handler(
				CorsHandler.create("*").allowedMethod(HttpMethod.GET)
						.allowedMethod(HttpMethod.POST)
						.allowedMethod(HttpMethod.OPTIONS)
						.allowedHeader("Content-Type, Authorization"));
		// registering different route handlers
		this.registerHandlers();

		//portConnection.getPort()
		server.requestHandler(router::accept).listen(port);
		
	}
	
	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {
		router.route(HttpMethod.GET, "/").blockingHandler(this::welcomeRoute);
		router.route(HttpMethod.POST, "/search").blockingHandler(this::search);
		router.route(HttpMethod.POST, "/homeSearch").blockingHandler(this::multiSearch);
		router.route(HttpMethod.POST, "/searchUser").blockingHandler(this::searchUser);
		router.route(HttpMethod.POST, "/searchUserRelation").blockingHandler(this::searchUserRelation);
		router.route(HttpMethod.POST, "/searchUserInfluence").blockingHandler(this::searchUserInfluence);
		router.route(HttpMethod.POST, "/indexTweets").blockingHandler(this::indexTweets);
		router.route(HttpMethod.POST, "/userInfo").blockingHandler(this::userInfoRoute);
		router.route(HttpMethod.POST, "/indexUser").blockingHandler(this::indexUsers);
		router.route(HttpMethod.POST, "/indexUserInfluence").blockingHandler(this::indexUserInfluence);
		router.route(HttpMethod.POST, "/retweet").blockingHandler(this::retweetRoute);
		router.route(HttpMethod.POST, "/muteUser").blockingHandler(this::muteRoute);
		router.route(HttpMethod.POST, "/blockUser").blockingHandler(this::blockRoute);		
		router.route(HttpMethod.POST, "/followUser").blockingHandler(this::followRoute);
		router.route(HttpMethod.POST, "/unfollowUser").blockingHandler(this::unfollowRoute);
		router.route(HttpMethod.POST, "/deleteTweets").blockingHandler(this::deleteTweets);

}
	
	
	/**
	 * welcome route
	 * 
	 * @param routingContext
	 */

	public void welcomeRoute(RoutingContext routingContext) {
		routingContext.response().end("<h1>Welcome To Twitter Tool</h1>");
		
	}

	
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void search(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void multiSearch(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				homeSearchBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search userRelation
	 * 
	 * @param routingContext
	 */
	public void searchUserRelation(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchUserRelationBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void searchUser(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchUserBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void searchUserInfluence(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchUserInfluenceBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void userInfoRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				userInfoRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to index tweets into elasticsearch 
	 * 
	 * @param routingContext
	 */
	public void indexTweets(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexTweetsBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void indexUsers(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexUsersBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void indexUserInfluence(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexUserInfluenceBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to retweet status
	 * 
	 * @param routingContext
	 */
	public void retweetRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				retweetBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void muteRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				muteRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to block User on twitter
	 * 
	 * @param routingContext
	 */
	public void blockRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				blockRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	/**
	 * use to follow User on twitter
	 * 
	 * @param routingContext
	 */
	public void followRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				followRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void unfollowRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				unfollowRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to delete documents
	 * 
	 * @param routingContext
	 */
	public void deleteTweets(final RoutingContext routingContext) {
		  Thread t=new Thread() {
		   public void run() {
			   
			  deleteTweetsBlocking(routingContext);
			
		   }
		  };
		  t.setDaemon(true);
		  t.start();
		 }

	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		
		
		String keyword = (routingContext.request().getParam("keyword") == null) ? "cat": routingContext.request().getParam("keyword");
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "tweet": routingContext.request().getParam("searchIn");
		String size = (routingContext.request().getParam("size") == null) ? "500": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
	    cal1.add(Calendar.DATE, -1);
	    String oneDayPreviousDate=  formatter.format(cal1.getTime()).toString();
	    cal2.add(Calendar.DATE, -2);
	    String secondDayDate =  formatter.format(cal2.getTime()).toString();
		Date date = new Date();
		String types[] ={formatter.format(date),oneDayPreviousDate,secondDayDate};
		
		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}
		int pageNo = 0;
		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		
		try {

			String[] fields = mapper.readValue(searchIn, String[].class);
			
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchTweetDocuments(elasticsearch.getESInstance(esHost, esPort), indexEncription,types, fields, keyword,pageNo, documentsSize,sortOn,order);

			Map<String, Object> totalCount = documents.get(0);
			documents.remove(0);
			responseMap.put("status", "success");
			responseMap.put("documents", documents);
			responseMap.put("size", totalCount.get("totalCount"));
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	   * use to delete tweets by Date 
	   * 
	   * @param routingContext
	   */
	
	  public void deleteTweetsBlocking(RoutingContext routingContext) {
	    Map<String, Object> responseMap = new HashMap<String, Object>();
	    String response;
	    ObjectMapper mapper = new ObjectMapper();
	    String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
	    String date= (routingContext.request().getParam("date") == null) ? "": routingContext.request().getParam("date");
	    
	    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	    Calendar cal = Calendar.getInstance();
	    cal.add(Calendar.DATE, -3);
	    String keyword=  format.format(cal.getTime()).toString();
	    if (date.isEmpty()) {
			date = keyword;
		}
	    
	    System.err.println("3 days pre date "+date);
	   
	    try {
	      boolean success = false;
	      success = this.elasticsearch.deleteMaping(elasticsearch.getESInstance(esHost, esPort),indexEncription,date);
	      if (success) {
	    	  responseMap.put("status", "true");
		}
	      else {
	    	  responseMap.put("status", "false");
		}
	      
	      response = mapper.writeValueAsString(responseMap);

	    } catch (Exception e) {
	      response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
	          + "}";
	    }

	    routingContext.response().end(response);

	  }

	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void mutualRelationBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String screenName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName");
		String id = (routingContext.request().getParam("id") == null) ? "tweet": routingContext.request().getParam("id");
		
		try {

			String getuserObject[] = {"userScreenName"};
			ArrayList<String> commonRelation = new ArrayList<String>();
		
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, getuserObject, screenName,0, documentsSize);
			if (documents.size() > 0 ) {
				commonRelation = (ArrayList<String>) documents.get(1).get("commonRelation");
				int bit = commonRelation.contains(id) ? 1:0 ;
				if ( bit == 1 ) {
					responseMap.put("status", "true");
				}
				else {
					responseMap.put("status", "false");
				}
			}
			
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void stickyRelationBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String screenName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName");
		String id = (routingContext.request().getParam("id") == null) ? "tweet": routingContext.request().getParam("id");
		
		try {

			String getuserObject[] = {"userScreenName"};
			ArrayList<Object> commonRelation = new ArrayList<Object>();
			ArrayList<Object> nonCommonRelation = new ArrayList<Object>();
		
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, getuserObject, screenName,0, documentsSize);
			if (documents.size() > 0 ) {
				commonRelation = (ArrayList<Object>) documents.get(1).get("commonRelation");
				nonCommonRelation = (ArrayList<Object>) documents.get(1).get("nonCommonFollowers");
				LinkedList<ArrayList<Object>> esTotalFollowers = new LinkedList<ArrayList<Object>>();
				esTotalFollowers.add(commonRelation);
				esTotalFollowers.add(nonCommonRelation);
				int check;
				boolean exist = false;
				for ( ArrayList<Object> esFollowers : esTotalFollowers) {

						check = esFollowers.contains(id) ? 1 : 0;
						
						if ( check == 1 ) {
							exist =	true ;
						}
						
					}
				
				if (exist) {
					responseMap.put("status","true");
				}
				else {
					responseMap.put("status","false");
				}
							
			}
					
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void homeSearchBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
	    cal1.add(Calendar.DATE, -1);
	    String oneDayPreviousDate=  formatter.format(cal1.getTime()).toString();
	    cal2.add(Calendar.DATE, -2);
	    String secondDayDate =  formatter.format(cal2.getTime()).toString();
		Date date = new Date();
		String types[] ={formatter.format(date),oneDayPreviousDate,secondDayDate};
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String size = (routingContext.request().getParam("size") == null) ? "500": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		int pageNo =0;
		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}
		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		try {	

			ArrayList<Map<String, Object>> documents = this.elasticsearch.homePageData(elasticsearch.getESInstance(esHost, esPort),indexEncription, types,pageNo, documentsSize,sortOn,order);
			Map<String, Object> totalCount = documents.get(0);
			documents.remove(0);
			responseMap.put("status", "success");
			responseMap.put("documents", documents);
			responseMap.put("size", totalCount.get("totalCount"));
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserRelationBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
		String keyword = (routingContext.request().getParam("keyword") == null) ? "": routingContext.request().getParam("keyword").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "userScreenName": routingContext.request().getParam("searchIn");
		
		try {

			String[] fields = mapper.readValue(searchIn, String[].class);
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, fields, keyword,0, 10);
			documents.remove(0);
			responseMap.put("status", "success");
			responseMap.put("documents", documents);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "followers": routingContext.request().getParam("searchIn").toLowerCase();
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String bit = (routingContext.request().getParam("bit") == null) ? "0": routingContext.request().getParam("bit");
		int pageNo =0;
		int	flagbit =0;
		
		if (!(page.isEmpty()) || !(bit.isEmpty())) {
			 pageNo = Integer.parseInt(page);
			flagbit =Integer.parseInt(bit);
		}
		
		try {
			long start = System.currentTimeMillis();
			String[] fields={"userScreenName"};
			String nonCommonRelation = "nonCommon"+searchIn.substring(0,1).toUpperCase()+searchIn.substring(1);
			
			ArrayList<Map<String,Object>> friendIds =this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, fields, indexName,0, 10);
			ArrayList<Object> commonIds = null;
			ArrayList<Object> nonCommonIds = null;
			if (!(friendIds.size()==0)) {
			commonIds = (ArrayList<Object>) friendIds.get(1).get("commonRelation");
			nonCommonIds = (ArrayList<Object>) friendIds.get(1).get(nonCommonRelation);
		}
			
			ArrayList<Map<String, Object>> commonDocuments =null;
			ArrayList<Map<String, Object>> nonCommonDocuments =null;
			String[] common = getIds(commonIds);
			String[] nonCommon = getIds(nonCommonIds);
			
			LinkedList<String []> commonRelationIds = chunksIds(common, 100);
			LinkedList<String []> nonCommonRelationids = chunksIds(nonCommon, 100);
			if (commonRelationIds.size() == 0 || nonCommonRelationids.size()==0) {
				responseMap.put("status", "no data found ");
				System.exit(0);
			}
			if (flagbit==0) {
				String [] relationIds = nonCommonRelationids.get(pageNo);
				nonCommonDocuments = this.elasticsearch.searchUserDocumentsByIds(elasticsearch.getESInstance(esHost, esPort),this.Index,searchIn,relationIds);
				responseMap.put("status", "success");
				responseMap.put("totalCount",nonCommonIds.size());
				responseMap.put("nonCommon", nonCommonRelationids.size());
				responseMap.put("nonCommonDocuments", nonCommonDocuments);
				
			} else {
				String [] relationIds = commonRelationIds.get(pageNo);
				commonDocuments = this.elasticsearch.searchUserDocumentsByIds(this.elasticsearch.getESInstance(this.esHost,this.esPort),this.Index, searchIn,relationIds);
				responseMap.put("status", "success");
				responseMap.put("totalCount",commonIds.size());
				responseMap.put("common", commonRelationIds.size());
				responseMap.put("commonDocuments", commonDocuments);
			}

			long end = System.currentTimeMillis()-start;
			System.out.println("time taken "+end);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserInfluenceBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String userScreenName = (routingContext.request().getParam("userScreenName") == null) ? "": routingContext.request().getParam("userScreenName").toLowerCase();
		String influenceScreenName = (routingContext.request().getParam("influenceScreenName") == null) ? "followers": routingContext.request().getParam("influenceScreenName").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "followers": routingContext.request().getParam("searchIn");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String bit = (routingContext.request().getParam("bit") == null) ? "0": routingContext.request().getParam("bit");
		int pageNo =0;
		int	flagBit = 0;
		if (!(page.isEmpty()) || !(bit.isEmpty())) {
			 pageNo = Integer.parseInt(page);
			flagBit = Integer.parseInt(bit);
		}

		try {
			
			String [] fields = {"userScreenName"};

			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, fields, userScreenName,0, 10);

			ArrayList<String> commonIds = new ArrayList<String>();
			ArrayList<String> nonCommonIds = new ArrayList<String>();
			
			if (documents.size() !=0) {
				commonIds =(ArrayList<String>) documents.get(1).get(userScreenName+influenceScreenName+"FollowerRelation");
				nonCommonIds = (ArrayList<String>) documents.get(1).get(userScreenName+influenceScreenName+"NonFollowerRelation");

		}
			String common[] = getArrayIds(commonIds);
			String nonCommon[] = getArrayIds(nonCommonIds);
			LinkedList<String []> commonRelationIds = chunksIds(common, 100);
			LinkedList<String []> nonCommonRelationIds = chunksIds(nonCommon, 100);
			if (flagBit ==0) {
				
				String [] relationIds = nonCommonRelationIds.get(pageNo);
				documents = this.elasticsearch.searchUserDocumentsByIds(elasticsearch.getESInstance(esHost, esPort),this.Index,searchIn,relationIds);
				responseMap.put("status", "success");
				responseMap.put("totalCount",nonCommonIds.size());
				responseMap.put("nonCommon", nonCommonRelationIds.size());
				responseMap.put("documents", documents);
			} else {
				
				String [] relationIds = commonRelationIds.get(pageNo);
				documents = this.elasticsearch.searchUserDocumentsByIds(elasticsearch.getESInstance(esHost, esPort),this.Index, searchIn,relationIds);
				responseMap.put("status", "success");
				responseMap.put("totalCount",commonIds.size());
				responseMap.put("common", commonRelationIds.size());
				responseMap.put("documents", documents);
			}
			
			
			
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to index user info for given credentials
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexUsersBlocking(RoutingContext routingContext) {
		String response = "";
		Map<String, Object> responseMap = new HashMap<String, Object>(); 
		ObjectMapper mapper = new ObjectMapper();
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");

		try {
			TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {	};
			HashMap<String, Object> credentials = mapper.readValue(credentialsJson, typeRef);
			ArrayList<Map<String, Object>> usersInfo = new ArrayList<Map<String,Object>>();
			String userId;
			long start = System.currentTimeMillis();
			ArrayList<Map<String, Object>> userScreeName = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), "");
			String credentialScreename = userScreeName.get(0).get("userScreenName").toString().toLowerCase();
								userId = userScreeName.get(0).get("id").toString();
								
			// indexing credentials info object into elasticsearch
			TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
			client.prepareUpdate(this.esIndex,"user",userId).setDoc(userScreeName.get(0)).setUpsert(userScreeName.get(0)).setRefresh(true).execute().actionGet();
			client.close();
			
			//getting credentials friends and followers ids
			ArrayList<String> followerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
			ArrayList<String> friendIds = this.getFriendIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
			
			// updating User relation
			 updateUserRelation(userId, friendIds, followerIds);
			 
			 System.err.println("Relation Updated...!");
			 
			 long followerids [] = getArrayListAslong(followerIds);
			 long friendids [] = getArrayListAslong(friendIds);
			 
			 LinkedList<long[]> followersChunks = chunks(followerids, 500);
			 LinkedList<long[]> friendsChunks = chunks(friendids, 500);
			 
			 // getting followers info by sending 500 ids and then indexing into elasticSearch
			 for (int i = 0; i < followersChunks.size(); i++) {
				 
				 usersInfo = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),followersChunks.get(i));
				
				this.indexInESearch(usersInfo,this.Index,"followers");
			
			}
			
			// getting friends info by sending 500 ids and then indexing into elasticSearch
			 
			 for (int i = 0; i < friendsChunks.size(); i++) {
				 
				 usersInfo = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),friendsChunks.get(i));
					
					this.indexInESearch(usersInfo,this.Index,"friends");
				
				}

			boolean success = false;

			String[] credentialFriendIds = getArrayIds(followerIds);
			String[] credentialFollowerIds = getArrayIds(friendIds);
			
			if (elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort), "user", "friends", credentialFriendIds) && elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort), "user", "followers", credentialFollowerIds) ) {
				success = true;
					}
	if (success) {
			long end = System.currentTimeMillis()-start;
			System.err.println("time taken total "+end);
			responseMap.put("status", "success");
			response = mapper.writeValueAsString(responseMap);
		}
			
				
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);

	}

/**
	 * use to index tweets in ES
	 * 
	 * @param tweets
 * @throws Exception 
	 */
		public void indexInESearch(ArrayList<Map<String, Object>> tweets,String indexName,String type)throws Exception {

			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			for (Map<String, Object> tweet : tweets) {
					bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));	
				}				
			bulkRequestBuilder.setRefresh(true).execute().actionGet();
			
			client.close();	
	
	}

		public void updateUserRelation(String userId,ArrayList<String> friendIds,ArrayList<String> followerIds) throws TwitterException{ // Update User
	
			
			try {	
			int var;
			ArrayList<String> friendsWhoCommon = new ArrayList<String>();
			ArrayList<String> friendsNonCommon = new ArrayList<String>();
			
			//finding common and nonCommon friends 
			for (int i = 0; i < friendIds.size(); i++) {
				var = followerIds.contains(friendIds.get(i)) ? 1 : 0;
				if (var == 1) {
					friendsWhoCommon.add(friendIds.get(i));
				} else {
					friendsNonCommon.add(friendIds.get(i));
				}
						
			}
			
			ArrayList<String> followersNonCommon = new ArrayList<String>();
			//finding  nonCommon followers 
			for (int i = 0; i < followerIds.size(); i++) {
				var = friendsWhoCommon.contains(followerIds.get(i)) ? 1 : 0;
				if (var == 0) {
					followersNonCommon.add(followerIds.get(i));
	
				}
			}
			
			
			
			
			String[]  userid= {userId};
			TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
			ArrayList<Map<String, Object>> unfollowed = this.elasticsearch.searchUserDocumentsByIds(client, this.esIndex, "user",userid);
			LinkedList<ArrayList<Object>> esTotalFollowers = null;
			
			//finding  followers who unfollowed me
			ArrayList<String> unfollowedFollowers = new ArrayList<String>();
			ArrayList<Object> commonFollower = new ArrayList<Object>();
			ArrayList<Object> nonCommonFollower = new ArrayList<Object>();
			if (unfollowed.size()!=0) {
				boolean enter = false;
				for (String fields : unfollowed.get(0).keySet()) {
					
					if (fields.equalsIgnoreCase("commonRelation") || fields.equalsIgnoreCase("nonCommonFollowers")) {

						commonFollower = (ArrayList<Object>) unfollowed.get(0).get("commonRelation");
						nonCommonFollower = (ArrayList<Object>) unfollowed.get(0).get("nonCommonFollowers");
						esTotalFollowers = new LinkedList<ArrayList<Object>>();
						esTotalFollowers.add(commonFollower);
						esTotalFollowers.add(nonCommonFollower);
						enter = true;
						
					}
					
				}
				if (enter) {
					int check ;
					for ( ArrayList<Object> esFollowers : esTotalFollowers) {
						
						for (int i = 0; i < esFollowers.size(); i++) {
							
							check = followerIds.contains(esFollowers.get(i)) ? 1 : 0;
							if (check == 0) {
								unfollowedFollowers.add(esFollowers.get(i).toString());						}
						}
					}
				}
			}
				
				
			
			Map<String, Object> updateRelation = new HashMap<String, Object>();
			updateRelation.put("commonRelation",friendsWhoCommon);
			updateRelation.put("nonCommonFriends",friendsNonCommon);
			updateRelation.put("nonCommonFollowers",followersNonCommon);
			updateRelation.put("unfollowedFollowers",unfollowedFollowers);
			TransportClient client1 = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			client1.prepareUpdate(this.esIndex,"user",userId).setDoc(updateRelation).setUpsert(updateRelation).setRefresh(true).execute().actionGet();
			client1.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * use to index user influencer info for given credentials
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexUserInfluenceBlocking(RoutingContext routingContext) {
		String response = "";
		Map<String, Object> responseMap = new HashMap<String, Object>();
		ObjectMapper mapper = new ObjectMapper();

		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");
		try {

			TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {	};
			HashMap<String, Object> credentials = mapper.readValue(credentialsJson, typeRef);
		
			ArrayList<Map<String, Object>> tweets;
			String userId;
			String influenceScreenName = screenName.toLowerCase().trim();
			long start = System.currentTimeMillis();
			
			ArrayList<Map<String, Object>> userName = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), "");
			String credentialScreename = userName.get(0).get("userScreenName").toString().toLowerCase();
								userId = userName.get(0).get("id").toString();
								
			ArrayList<String> credentialFollowerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
			ArrayList<String>influenceFollowerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);
			
			long InfluenceFollowerids [] = getArrayListAslong(influenceFollowerIds);
			
			 updateUserInfluencerRelation(credentialScreename, influenceScreenName, userId,credentialFollowerIds,influenceFollowerIds);
			 
			 System.err.println("Relation Updated...!");
			 
			 ArrayList<Map<String, Object>> credentialsInfo = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);			
			 TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
			 client.prepareUpdate("twitter","user",credentialsInfo.get(0).get("id").toString()).setDoc(credentialsInfo.get(0)).setUpsert(credentialsInfo.get(0)).setRefresh(true).execute().actionGet();
			 client.close();
			 
			 LinkedList<long[]> chunks = chunks(InfluenceFollowerids, 500);
			 
			 for (int i = 0; i < chunks.size(); i++) {
				 
				tweets = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),chunks.get(i));
				
				this.indexInESearch(tweets,this.Index,"followers");
			
			}
			
			boolean success = false;
			String[] credentialIds = getArrayIds(credentialFollowerIds);
			String[] influenceIds = getArrayIds(influenceFollowerIds);
				if (elasticsearch.documentsExist(elasticsearch.getESInstance(this.esHost, this.esPort), "user", "followers", influenceIds) && elasticsearch.documentsExist(elasticsearch.getESInstance(this.esHost, this.esPort), "user", "followers", credentialIds) ) {
					success = true;
				}
				
			if (success) {
			long end = System.currentTimeMillis()-start;
			System.err.println("time taken total "+end);
			responseMap.put("status", "success");
			response = mapper.writeValueAsString(responseMap);
		}
			
				
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);

	}
	
	public boolean updateUserInfluencerRelation(String credentialScreeName,String influncerScreenName,String userId,ArrayList<String> credentialFollowerIds,ArrayList<String> influenceFollowerIds) throws Exception { // Update User
		boolean success =false;
		try {
			// updating  credential user data   with common followers with influence
			ArrayList<String> followersWhoCommon = new ArrayList<String>();
			ArrayList<String> followersNonCommon = new ArrayList<String>();
			
			for (int i = 0; i < influenceFollowerIds.size(); i++) {
				int var = credentialFollowerIds.contains(influenceFollowerIds.get(i)) ? 1 : 0;
				if (var == 1) {
					followersWhoCommon.add(influenceFollowerIds.get(i));
				} else {
					followersNonCommon.add(influenceFollowerIds.get(i));
				}
			}
			
			Collections.sort(followersNonCommon.subList(0, followersNonCommon.size()));
			Collections.sort(followersWhoCommon.subList(0, followersWhoCommon.size()));
			
			Map<String, Object> updatefollowerRelation = new HashMap<String, Object>();
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"FollowerRelation",followersWhoCommon);
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"NonFollowerRelation",followersNonCommon);
			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			client.prepareUpdate("twitter","user",userId).setDoc(updatefollowerRelation).setUpsert(updatefollowerRelation).setRefresh(true).execute().actionGet();
			client.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		return success;
	}
	
	public static LinkedList<long[]> chunks(long[] bigList, int n) {
		int partitionSize = n;
		
		LinkedList<long[]> partitions = new LinkedList<long[]>();
		for (int i = 0; i < bigList.length; i += partitionSize) {
			long[] bulk = Arrays.copyOfRange(bigList, i,
					Math.min(i + partitionSize, bigList.length));
			partitions.add(bulk);
		}

		return partitions;
	}

	public static  long[] getArrayListAslong(ArrayList<String> ids){
		Collections.sort(ids.subList(0, ids.size()));
		  long [] id =new long[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] =Long.parseLong(ids.get(i).toString());
		}
		  
		  return id;
	  }
	
	/**
	 * use to index tweets for given keyword
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexTweetsBlocking(RoutingContext routingContext) {
		String response = "";
		int keywordsIndex = 0;
		int credentialsIndex = 0;
		//making index type date wise so we can easily delete data older than 3 days
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		String type = formatter.format(date);
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String keywordsJson = (routingContext.request().getParam("keywords") == null) ? "['cricket', 'football']": routingContext.request().getParam("keywords");
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "[]" : routingContext.request().getParam("credentials");
		long start = System.currentTimeMillis();
		try {

			String[] keywords = mapper.readValue(keywordsJson, String[].class);
			TypeReference<ArrayList<HashMap<String, Object>>> typeRef = new TypeReference<ArrayList<HashMap<String, Object>>>() {};
			ArrayList<HashMap<String, Object>> credentials = mapper.readValue(credentialsJson, typeRef);
			if (keywords.length == 0 || credentials.size() == 0 || indexEncription.isEmpty()) {
				response = "correctly pass keywords or credentials ";
			} else {
				
				while (keywordsIndex < keywords.length) {

					if (credentialsIndex > credentials.size() - 1)
						credentialsIndex = 0;

					Map<String, Object> credentialsMap = credentials.get(credentialsIndex);
					
					ArrayList<Map<String, Object>> tweets = this.searchTweets(this.getTwitterInstance((String) credentialsMap.get("consumerKey"),(String)credentialsMap.get("consumerSecret"),(String) credentialsMap.get("accessToken"),(String) credentialsMap
					.get("accessTokenSecret")),	keywords[keywordsIndex]);
					
					if (tweets.size() == 0) {
						keywordsIndex--;
					}
					
					LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
					for (int i = 0; i < tweets.size(); i += bulkSize) {
						ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(
								tweets.subList(i,
										Math.min(i + bulkSize, tweets.size())));
						bulks.add(bulk);
					}
					
					for (ArrayList<Map<String, Object>> tweetsList : bulks) {
						this.indexInES(indexEncription,type,tweetsList);
					}
					keywordsIndex++;
					credentialsIndex++;

				}
			}
			System.err.println("time taken "+ (System.currentTimeMillis()-start));
			response = "{\"status\" : \"success\"}";
			
		} catch (Exception ex) {
			response = 	"{\"status\" : \"error\", \"msg\" :" + ex.getMessage()+ "}";
		}
		routingContext.response().end(response);

	}
	
	
/**
	 * use to index tweets in ES
	 * 
	 * @param tweets
 * @throws Exception 
	 */
	public void indexInES(String indexName,String type,ArrayList<Map<String, Object>> tweets) throws Exception {
		TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);

		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Map<String, Object> tweet : tweets) {
//			System.out.println(tweet);
			bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet)
					.setUpsert(tweet));

		}
		bulkRequestBuilder.setRefresh(true).execute().actionGet();

		client.close();
	}
	 public String[] getIds(ArrayList<Object> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
	 
	 public String[] getArrayIds(ArrayList<String> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
	  
	  public  LinkedList<String[]> chunksIds(String [] bigList, int n) {
			int partitionSize = n;
			LinkedList<String[]> partitions = new LinkedList<String[]>();
			for (int i = 0; i < bigList.length; i += partitionSize) {
				String[] bulk = Arrays.copyOfRange(bigList, i,
						Math.min(i + partitionSize, bigList.length));
				partitions.add(bulk);
			}

			return partitions;
		}

	/**
	 * use to get es instance
	 * 
	 * @param esHost
	 * @param esPort
	 * @return
	 * @throws UnknownHostException
	 */
		public TransportClient esClient(String esHost, int esPort)
			throws UnknownHostException {
		TransportClient client = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(esHost,
						esPort));
		return client;
	}
		
		
	/**
	 * route for retweets
	 * 
	 * @param routingContext
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
		public void retweetBlocking(RoutingContext routingContext) {

		String response;
		ObjectMapper mapper = new ObjectMapper();
		String tweetsId = (routingContext.request().getParam("tweetsId") == null) ? "[]": routingContext.request().getParam("tweetsId");
		String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "[]" : routingContext.request().getParam("credentials");
		try {
			TypeReference<ArrayList<Long>> typeRef = new TypeReference<ArrayList<Long>>() {};
			ArrayList<Long> tweetsIdsList = mapper.readValue(tweetsId, typeRef);

			TypeReference<HashMap<String, Object>> credentialsTypeReference = new TypeReference<HashMap<String, Object>>() {};
			HashMap<String, Object> credentials = mapper.readValue(credentialsjson, credentialsTypeReference);

			ArrayList<Long> retweetIds = twitter4jApi.retweet(this.getTwitterInstance(credentials.get("consumerKey").toString(), credentials.get("consumerSecret").toString(), credentials.get("accessToken")
							.toString(), credentials.get("accessTokenSecret")
							.toString()), tweetsIdsList);
			response = mapper.writeValueAsString(retweetIds);

		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

	/**
	 * use to follow by id
	 * 
	 * @param routingContext
	 * @param routingContext
	 *            can't follow more than 1000 user in one day, total 5000 users
	 *            can be followed by a account
	 */
		public void followRouteBlocking(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String userIds = (routingContext.request().getParam("screenNames") == null) ? ""
				: routingContext.request().getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {};
			HashMap<String, String> credentials = mapper.readValue(credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> followIdsType = new TypeReference<ArrayList<String>>() {};
			ArrayList<String> followIds = mapper.readValue(userIds,followIdsType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.getFriendShip(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),
					credentials.get("accessToken"),
					credentials.get("accessTokenSecret")), followIds);
			responseMap.put("following", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

	/**
	 * use to unfollow by id
	 * 
	 * @param routingContext
	 * @param credentials
	 * @param userIds
	 *            can't follow more than 1000 user in one day, total 5000 users
	 *            can be followed by a account
	 */
		public void unfollowRouteBlocking(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String userIds = (routingContext.request().getParam("screenNames") == null) ? ""
				: routingContext.request().getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {	};
			HashMap<String, String> credentials = mapper.readValue(credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> followIdsType = new TypeReference<ArrayList<String>>() {};
			ArrayList<String> followIds = mapper.readValue(userIds,followIdsType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.destroyFriendShip(this.getTwitterInstance(credentials.get("consumerKey"),
							credentials.get("consumerSecret"),
							credentials.get("accessToken"),
							credentials.get("accessTokenSecret")), followIds);
			responseMap.put("unfollowing", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

	/**
	 * use to mute by using screenName
	 * 
	 * @param routingContext
	 * @param credentials
	 * @param ScreenName
	 *            return name of those on which action has been taken
	 */
		public void muteRouteBlocking(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String friendsListJson = (routingContext.request().getParam("screenNames") == null) ? "" : routingContext.request()
				.getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {
			};
			HashMap<String, String> credentials = mapper.readValue(
					credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> friendsListType = new TypeReference<ArrayList<String>>() {
			};
			ArrayList<String> friendsList = mapper.readValue(friendsListJson,
					friendsListType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.muteUser(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),credentials.get("accessToken"),credentials.get("accessTokenSecret")), friendsList);
			responseMap.put("muted", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

	/**
	 * use to block by using screenName
	 * 
	 * @param routingContext
	 * @param credentials
	 * @param ScreenName
	 *            return name of those on which action has been taken
	 */
		public void blockRouteBlocking(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String friendsListJson = (routingContext.request().getParam(
				"screenNames") == null) ? "" : routingContext.request()
				.getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {
			};
			HashMap<String, String> credentials = mapper.readValue(
					credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> friendsListType = new TypeReference<ArrayList<String>>() {
			};
			ArrayList<String> friendsList = mapper.readValue(friendsListJson,
					friendsListType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.blockUser(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),credentials.get("accessToken"),credentials.get("accessTokenSecret")), friendsList);
			responseMap.put("blocked", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}
		
		/**
		 * use to get userInfo
		 * 
		 * @param routingContext
		 */
		public void userInfoRouteBlocking(RoutingContext routingContext) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			String response = null;
			ObjectMapper mapper = new ObjectMapper();
			String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
			String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");

			try {
				TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {	};
				HashMap<String, Object> credentials = mapper.readValue(credentialsjson, typeRef);
				ArrayList<Map<String, Object>> documents = null;
				documents = this.userInfo(this.getTwitterInstance(credentials.get("consumerKey").toString(),credentials.get("consumerSecret").toString(),credentials.get("accessToken").toString(),credentials.get("accessTokenSecret").toString()),screenName);
				if (documents.size()!=0) {
				this.elasticsearch.getESInstance(this.esHost, this.esPort).prepareUpdate("twitter","user",documents.get(0).get("id").toString()).setDoc(documents.get(0)).setUpsert(documents.get(0)).setRefresh(true).execute().actionGet();
				this.elasticsearch.getESInstance(this.esHost, this.esPort).close();
				responseMap.put("userInfo", documents);
				responseMap.put("size", documents.size());
				response = mapper.writeValueAsString(responseMap);
				
			}
				else {
					responseMap.put("status", "fail");
					response = mapper.writeValueAsString(responseMap);
				}
				
			
			} catch (Exception e) {
				response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
						+ "}";
			}

			routingContext.response().end(response);

		}

	/**
		 * use to get tweets
		 * 
		 * @param twitter
		 * @param query
		 * @return list of tweets
		 * @throws TwitterException
		 * @throws Exception
		 */

		public ArrayList<Map<String, Object>> userInfo(Twitter twitter,String screenName) throws TwitterException,Exception {
			ArrayList<Map<String, Object>> tweets = twitter4jApi.getUserInfo(twitter,screenName);
			return tweets;

		}

	/**
	 * use to Mute User
	 * 
	 * 
	 * @param credentials
	 * @param screenName
	 * @return name of those on which actio has benn taken
	 */
		public ArrayList<String> muteUser(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException {

		return twitter4jApi.muteUser(twitter, ScreenName);
	}

	/**
	 * use to Block User
	 * 
	 * @param credentials
	 * @param screenName
	 * @return name of those on which actio has benn taken
	 */
	public ArrayList<String> blockUser(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException {

		return twitter4jApi.blockUser(twitter, ScreenName);
	}

	/**
	 * use to get tweets
	 * 
	 * @param twitter
	 * @param query
	 * @return list of tweets
	 * @throws TwitterException
	 * @throws Exception
	 */

	public ArrayList<Map<String, Object>> searchTweets(Twitter twitter,
			String keyword) throws Exception {
		ArrayList<Map<String, Object>> tweets = twitter4jApi.search(twitter,
				keyword);
		return tweets;

	}
	/**
	 * use to get tweets
	 * 
	 * @param twitter
	 * @param query
	 * @return list of tweets
	 * @throws TwitterException
	 * @throws Exception
	 */

	public ArrayList<Map<String, Object>> search(Twitter twitter,
			String keyword) throws Exception {
		ArrayList<Map<String, Object>> tweets = twitter4jApi.search(twitter,
				keyword);
		return tweets;

	}


	

	/**
	 * use to get followerslist
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException

	 * @throws Exception
	 */
	public ArrayList<Map<String, Object>> getUsersInfoByIds(Twitter twitter,long []influenceFollowerIds) throws IllegalStateException,TwitterException {

		return twitter4jApi.getUsersInfoByIds(twitter,influenceFollowerIds);
	}
	

	/**
	 * use to get followerIds
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public ArrayList<String> getFollowerIds(Twitter twitter, String screenName) throws TwitterException {

		return twitter4jApi.getFollowerIds(twitter, screenName);
	}
	
	/**
	 * use to get friendIds
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public ArrayList<String> getFriendIds(Twitter twitter, String screenName) throws TwitterException {

		return twitter4jApi.getFriendIds(twitter, screenName);
	}



	/**
	 * use to create friendship
	 * 
	 * @param twitter
	 * @param user
	 *            Screen Name
	 * @return friended data about user
	 * @throws TwitterException
	 * @throws Exception
	 */
	public ArrayList<String> getFriendShip(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException, Exception {

		return twitter4jApi.createFriendship(twitter, ScreenName);
	}

	/**
	 * use to create friendship
	 * 
	 * @param twitter
	 * @param user
	 *            Screen Name
	 * @return friended data about user
	 * @throws TwitterException
	 * @throws Exception
	 */
	public ArrayList<String> destroyFriendShip(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException, Exception {

		return twitter4jApi.destroyFriendship(twitter, ScreenName);
	}

	/**
	 * use to get twitter instance
	 * 
	 * @param consumerKey
	 * @param consumerSecret
	 * @param accessToken
	 * @param accessTokenSecret
	 * @return
	 */

	public Twitter getTwitterInstance(String consumerKey,String consumerSecret, String accessToken, String accessTokenSecret) throws TwitterException {
		return twitter4jApi.getTwitterInstance(consumerKey, consumerSecret,
				accessToken, accessTokenSecret);
	}
}
