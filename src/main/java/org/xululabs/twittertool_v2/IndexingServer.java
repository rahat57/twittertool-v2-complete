package org.xululabs.twittertool_v2;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.io.IOException;
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

import javax.naming.SizeLimitExceededException;
import javax.swing.text.html.parser.Entity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.TwitterException;

public class IndexingServer extends AbstractVerticle {
	private static Logger log = LogManager.getRootLogger();
	HttpServer server;
	Router router;
	Twitter4jApi twitter4jApi;
	String host;
	int port;
	ElasticsearchApi elasticsearch;
	String esHost;
	String esIndex;
	String Index;
	int esPort;
	int bulkSize = 700;

	/**
	 * constructor use to initialize values
	 */
	 public  IndexingServer()  {
			this.host = "localhost";
			this.port =8182;
			this.twitter4jApi = new Twitter4jApi();
			this.elasticsearch = new ElasticsearchApi();
			this.esHost = "localhost";
			this.esPort = 9300;
			this.esIndex = "twitter";
			this.Index = "user"; 
			this.bulkSize = 500;
		
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
		router.route(HttpMethod.POST, "/indexTweets").blockingHandler(this::indexTweets);
		router.route(HttpMethod.POST, "/userInfo").blockingHandler(this::userInfoRoute);
		router.route(HttpMethod.POST, "/stickyInfo").blockingHandler(this::indexUserInfoRoute);
		router.route(HttpMethod.POST, "/indexUser").blockingHandler(this::indexUsers);
		router.route(HttpMethod.POST, "/indexUserInfluence").blockingHandler(this::indexUserInfluence);
		router.route(HttpMethod.POST, "/retweet").blockingHandler(this::retweetRoute);
		router.route(HttpMethod.POST, "/muteUser").blockingHandler(this::muteRoute);
		router.route(HttpMethod.POST, "/blockUser").blockingHandler(this::blockRoute);		
		router.route(HttpMethod.POST, "/followUser").blockingHandler(this::followRoute);
		router.route(HttpMethod.POST, "/unfollowUser").blockingHandler(this::unfollowRoute);

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
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void indexUserInfoRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexUserInfoRouteBlocking(routingContext);
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
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
			
		try {
			
					if (indexEncription.isEmpty()) {
						responseMap.put("index missing", "please give the value of the indexEncription");
						log.warn("index missing", "please give the value of the indexEncription");
	
					}
					
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
			client.prepareUpdate(indexEncription,"user",userId).setDoc(userScreeName.get(0)).setUpsert(userScreeName.get(0)).setRefresh(true).execute().actionGet();
			client.close();
			
			//getting credentials friends and followers ids
			ArrayList<String> followerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
			ArrayList<String> friendIds = this.getFriendIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
			
			// updating User relation
			Map<String, ArrayList<String>> relationsIds = updateUserRelation(indexEncription,userId, friendIds, followerIds);
			 
			 System.err.println("Relation Updated...!");
			 
			 long commonRelation [] = getArrayListAslong(relationsIds.get("commonRelation"));
			 long nonCommonFriends [] = getArrayListAslong(relationsIds.get("nonCommonFriends"));
			 long nonCommonFollowers [] = getArrayListAslong(relationsIds.get("nonCommonFollowers"));
			 
			 LinkedList<long[]> commonRelationChunks = chunks(commonRelation, 500);
			 LinkedList<long[]> nonCommonFriendsChunks = chunks(nonCommonFriends, 500);
			 LinkedList<long[]> nonCommonFollowersChunks = chunks(nonCommonFollowers, 500);
			 
			// getting common relations id's info by sending 500 ids and then indexing into elasticSearch
			 for (int i = 0; i < commonRelationChunks.size(); i++) {

				 usersInfo = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),commonRelationChunks.get(i));
				
				this.indexInESearch(usersInfo,indexEncription,"commonrelation");
			
			}
			 
			 // getting followers info by sending 500 ids and then indexing into elasticSearch
			 for (int i = 0; i <nonCommonFollowersChunks.size(); i++) {
				 usersInfo = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),nonCommonFollowersChunks.get(i));
				
				this.indexInESearch(usersInfo,indexEncription,"noncommonfollowers");
			
			}
			
			// getting friends info by sending 500 ids and then indexing into elasticSearch
			 
			 for (int i = 0; i <  nonCommonFriendsChunks.size(); i++) {
				 usersInfo = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),nonCommonFriendsChunks.get(i));
					
					this.indexInESearch(usersInfo,indexEncription,"noncommonfriends");
				
				}

			boolean success = false;

			String[] credentialcommonIds = getArrayIds(relationsIds.get("commonRelation"));
			String[] credentialNonCommonFollowerIds = getArrayIds(relationsIds.get("nonCommonFollowers"));
			String[] credentialNonCommonFriendsIds = getArrayIds(relationsIds.get("nonCommonFriends"));
			
			if (elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, "commonrelation", credentialcommonIds) && elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, "noncommonfollowers", credentialNonCommonFollowerIds)&& elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, "noncommonfriends", credentialNonCommonFriendsIds) ) {
				success = true;
					}
	if (success) {
			long end = System.currentTimeMillis()-start;
			log.info("time taken for user Indexing total "+end);
			responseMap.put("status", "success");
			response = mapper.writeValueAsString(responseMap);
		}
			
				
		} catch (Exception ex) {
			
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());
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
				int count =1;
			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			for (Map<String, Object> tweet : tweets) {
//				System.err.println(count++ + tweet.toString());
					bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));	
				}				
			bulkRequestBuilder.setRefresh(true).execute().actionGet();
			
			client.close();	
	
	}

		public Map<String,ArrayList<String>> updateUserRelation(String indexEncription,String userId,ArrayList<String> friendIds,ArrayList<String> followerIds) throws TwitterException{ // Update User
	
			Map<String,ArrayList<String>> relationIds = new HashMap<String,ArrayList<String>>();
			try {	
			int var;
			ArrayList<String> commonRelation = new ArrayList<String>();
			ArrayList<String> friendsNonCommon = new ArrayList<String>();
			
			//finding common and nonCommon friends 
			for (int i = 0; i < friendIds.size(); i++) {
				var = followerIds.contains(friendIds.get(i)) ? 1 : 0;
				if (var == 1) {
					commonRelation.add(friendIds.get(i));
				} else {
					friendsNonCommon.add(friendIds.get(i));
				}
						
			}
			
			ArrayList<String> followersNonCommon = new ArrayList<String>();
			//finding  nonCommon followers 
			for (int i = 0; i < followerIds.size(); i++) {
				var = commonRelation.contains(followerIds.get(i)) ? 1 : 0;
				if (var == 0) {
					followersNonCommon.add(followerIds.get(i));
	
				}
			}
					
			String[]  userid= {userId};
			TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
			ArrayList<Map<String, Object>> unfollowed = this.elasticsearch.searchUserDocumentsByIds(client,indexEncription, "user",userid);
			LinkedList<ArrayList<String>> esTotalFollowers = null;
			
			//finding  followers who unfollowed me
			ArrayList<String> unfollowedFollowers = new ArrayList<String>();
			ArrayList<String> esCommonRelation = new ArrayList<String>();
			ArrayList<String> esNonCommonFollower = new ArrayList<String>();
			ArrayList<String> esNonCommonFriend = new ArrayList<String>();
			boolean enter = false;
			if (unfollowed.size()!=0) {
				
				for (String fields : unfollowed.get(0).keySet()) {
					
					if (fields.equalsIgnoreCase("commonRelation") || fields.equalsIgnoreCase("nonCommonFollowers")) {

						esCommonRelation = (ArrayList<String>) unfollowed.get(0).get("commonRelation");
						esNonCommonFollower = (ArrayList<String>) unfollowed.get(0).get("nonCommonFollowers");
						esNonCommonFriend = (ArrayList<String>) unfollowed.get(0).get("nonCommonFriends");
						esTotalFollowers = new LinkedList<ArrayList<String>>();
						esTotalFollowers.add(esCommonRelation);
						esTotalFollowers.add(esNonCommonFollower);
						enter = true;
						
					}
					
				}
				if (enter) {
					int check ;
					for ( ArrayList<String> esFollowers : esTotalFollowers) {
						
						for (int i = 0; i < esFollowers.size(); i++) {
							
							check = followerIds.contains(esFollowers.get(i)) ? 1 : 0;
							if (check == 0) {
								unfollowedFollowers.add(esFollowers.get(i).toString());						}
						}
					}
					
					//updating all types mutualRelation ,nonCommonFriends,nonCommonFollowers so user get only latest data
					updateMappings(indexEncription,commonRelation, followersNonCommon, friendsNonCommon,esCommonRelation,esNonCommonFollower,esNonCommonFriend);
				}
				
			}
		
			relationIds.put("commonRelation", commonRelation);
			relationIds.put("nonCommonFriends", friendsNonCommon);
			relationIds.put("nonCommonFollowers", followersNonCommon);
			
			Map<String, Object> updateRelation = new HashMap<String, Object>();
			updateRelation.put("commonRelation",commonRelation);
			updateRelation.put("nonCommonFriends",friendsNonCommon);
			updateRelation.put("nonCommonFollowers",followersNonCommon);
			updateRelation.put("unfollowedFollowers",unfollowedFollowers);
			TransportClient client1 = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			client1.prepareUpdate(indexEncription,"user",userId).setDoc(updateRelation).setUpsert(updateRelation).setRefresh(true).execute().actionGet();
			client1.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		return relationIds;
	}
	
	public boolean	updateMappings(String indexEncription,ArrayList<String> commonRelation,ArrayList<String> followersNonCommon,ArrayList<String> friendsNonCommon,ArrayList<String> esCommonRelation,ArrayList<String> esNonCommonFollower,ArrayList<String> esNonCommonFriend){
		boolean success = false;
		
		//finding followers who Unfollowed in mutual relation
		ArrayList<String> mutualUnFollowed = new ArrayList<String>();
		try {
			int check ;
			for (int i = 0; i < esCommonRelation.size(); i++) {
				
				check = commonRelation.contains(esCommonRelation.get(i)) ? 1 : 0;
				if (check == 0) {
					mutualUnFollowed.add(esCommonRelation.get(i).toString());						}
			}
		
			
		// getting those who Unfolloed me in mutualRelation type
		String [] mutualUnFollowedIds = getArrayIds(mutualUnFollowed);
	if (mutualUnFollowedIds.length >= 1) {
		ArrayList<Map<String, Object>> copycommonFollowers = this.elasticsearch.searchUserDocumentsByIds(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription , "commonrelation", mutualUnFollowedIds);
		
		  // keeping mutualRelation unFollowed record in unfollowedFollowers type and deleting from mutualRelation
			indexInESearch(copycommonFollowers, indexEncription, "unfollowedfollowers");
			
			// deleting from mutualRelation
			deleteFromES(indexEncription, "commonrelation", mutualUnFollowed);
	}
		

		//finding followers who Unfollowed in nonCommonFollowers
		ArrayList<String> nonCommonUnFollowedFollowers = new ArrayList<String>();
		
			for (int i = 0; i < esNonCommonFollower.size(); i++) {
				
				check = followersNonCommon.contains(esNonCommonFollower.get(i)) ? 1 : 0;
				if (check == 0) {
					nonCommonUnFollowedFollowers.add(esNonCommonFollower.get(i).toString());						}
			}
		
			// getting those who Unfolloed me in noncommonfollowers type
			String [] nonCommonUnFollowedFollowersIds = getArrayIds(nonCommonUnFollowedFollowers);
			if (nonCommonUnFollowedFollowersIds.length >= 1) {
				
				ArrayList<Map<String, Object>> copyUnfollowedNonCommonFollowers = this.elasticsearch.searchUserDocumentsByIds(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription , "noncommonfollowers", nonCommonUnFollowedFollowersIds);
				
				// keeping mutualRelation unFollowed record in unfollowedFollowers type and deleting from mutualRelation
				indexInESearch(copyUnfollowedNonCommonFollowers, indexEncription, "unfollowedfollowers");
					
					// deleting from mutualRelation

					deleteFromES(indexEncription, "noncommonfollowers", nonCommonUnFollowedFollowers);

			}
				
		//finding friends who Unfollowed in nonCommonFriends
		
		ArrayList<String> nonCommonUnFollowedFriends = new ArrayList<String>();
		
			for (int i = 0; i < esNonCommonFriend.size(); i++) {
				
				check = friendsNonCommon.contains(esNonCommonFriend.get(i)) ? 1 : 0;
				if (check == 0) {
					nonCommonUnFollowedFriends.add(esNonCommonFriend.get(i).toString());						}
			}
			
		// getting those who Unfolloed me in noncommonfriends type
		String [] nonCommonUnFollowedFriendsIds = getArrayIds(nonCommonUnFollowedFriends);
	
				if (nonCommonUnFollowedFriendsIds.length >= 1) {
					// deleting from mutualRelation
					deleteFromES(indexEncription, "noncommonfriends", nonCommonUnFollowedFriends);
				}		
		
		
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
			
		return success;
	}
		
	 public String[] getIds(ArrayList<Object> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
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
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");
		try {

			TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {	};
			HashMap<String, Object> credentials = mapper.readValue(credentialsJson, typeRef);
		
			
			String userId;
			String influenceScreenName = screenName.toLowerCase().trim();
			long start = System.currentTimeMillis();
			
			ArrayList<Map<String, Object>> userName = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), "");
			String credentialScreename = userName.get(0).get("userScreenName").toString().toLowerCase();
								userId = userName.get(0).get("id").toString();
								
			ArrayList<String> credentialFollowerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
			ArrayList<String>influenceFollowerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);
			
			
		//finding common and Non common followers ids and saving into credentials user object
			
			Map<String, ArrayList<String>> relationIds = updateUserInfluencerRelation(indexEncription,credentialScreename, influenceScreenName, userId,credentialFollowerIds,influenceFollowerIds);
		
			long InfluenceCommonFollowerids [] = getArrayListAslong(relationIds.get("commonRelation"));
			long InfluenceNonCommonFollowerids [] = getArrayListAslong(relationIds.get("nonCommonRelation"));
			
			System.err.println("Relation Updated...!");
			 
			 ArrayList<Map<String, Object>> credentialsInfo = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);			
			 TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
			 client.prepareUpdate(indexEncription,"user",credentialsInfo.get(0).get("id").toString()).setDoc(credentialsInfo.get(0)).setUpsert(credentialsInfo.get(0)).setRefresh(true).execute().actionGet();
			 client.close();
			 
			 LinkedList<long[]> commonFollowerschunks = chunks(InfluenceCommonFollowerids, 500);
			 LinkedList<long[]> nonCommonFollowerschunks = chunks(InfluenceNonCommonFollowerids, 500);
			 
			 // indexing influencer credentials common followers into elasticsearch in type [credentaialsScreenName+influenceScreennamecommonfollowers] 
			 for (int i = 0; i < commonFollowerschunks.size(); i++) {
				 
				 ArrayList<Map<String, Object>> tweets = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),commonFollowerschunks.get(i));
				
				this.indexInESearch(tweets,indexEncription,credentialScreename+influenceScreenName+"commonfollowers");
			
			}
			 
			 // indexing influencer credentials NonCommon followers into elasticsearch in type [credentaialsScreenName+influenceScreennameNonCommonfollowers] 
			 for (int i = 0; i < nonCommonFollowerschunks.size(); i++) {
				 
				 ArrayList<Map<String, Object>> tweets = this.getUsersInfoByIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),nonCommonFollowerschunks.get(i));
				
				this.indexInESearch(tweets,indexEncription,credentialScreename+influenceScreenName+"noncommonfollowers");
			
			}
			
			boolean success = false;
			String[] commonFollowersIds = getArrayIds(relationIds.get("commonRelation"));
			String[] nonCommonFollowerIds = getArrayIds(relationIds.get("nonCommonRelation"));
				if (elasticsearch.documentsExist(elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, credentialScreename+influenceScreenName+"commonfollowers", commonFollowersIds) && elasticsearch.documentsExist(elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, credentialScreename+influenceScreenName+"noncommonfollowers", nonCommonFollowerIds) ) {
					success = true;
				}
				
			if (success) {
			long end = System.currentTimeMillis()-start;
//			System.err.println("time taken total "+end);
			log.info("time taken for user influence indexing total "+end);
			responseMap.put("status", "success");
			response = mapper.writeValueAsString(responseMap);
			}
						
		} catch (Exception ex) {
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);

	}
	
	/**
	 * use to delete records in ES
	 * 
	 * @param friends followers ids
 * @throws Exception 
	 */
	public void deleteFromES(String indexName,String type,ArrayList<String> ids) throws Exception {
		TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (String id : ids) {
			bulkRequestBuilder.add(client.prepareDelete(indexName,type,id));
		}
		bulkRequestBuilder.setRefresh(true).execute().actionGet();

		client.close();
	}
	
	public Map<String, ArrayList<String>> updateUserInfluencerRelation(String indexEncription,String credentialScreeName,String influncerScreenName,String userId,ArrayList<String> credentialFollowerIds,ArrayList<String> influenceFollowerIds) throws Exception { // Update User

		Map<String, ArrayList<String>> relationIds = new HashMap<String, ArrayList<String>>();
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
			
			// checking id already exist then deleting previous data
			String[]  userid= {userId};
			TransportClient client1 = elasticsearch.getESInstance(this.esHost, this.esPort);
			ArrayList<Map<String, Object>> unfollowed = this.elasticsearch.searchUserDocumentsByIds(client1,indexEncription, "user",userid);
			boolean enter=false;
			ArrayList<String> esCommonFollower = new ArrayList<String>();
			ArrayList<String> esNonCommonFollower = new ArrayList<String>();
			if (unfollowed.size() > 0 ) {
				
				for (String field : unfollowed.get(0).keySet()) {
			if (field.equalsIgnoreCase(credentialScreeName+influncerScreenName+"FollowerRelation") || field.equalsIgnoreCase(credentialScreeName+influncerScreenName+"NonFollowerRelation")) {
				esCommonFollower = (ArrayList<String>) unfollowed.get(0).get(credentialScreeName+influncerScreenName+"FollowerRelation");
				esNonCommonFollower = (ArrayList<String>) unfollowed.get(0).get(credentialScreeName+influncerScreenName+"NonFollowerRelation");
				enter=true;
				
					}
				}
				
			}
			
			ArrayList<String> commonUnFollowed = new ArrayList<String>();
			ArrayList<String> nonCommonUnFollowed = new ArrayList<String>();
			
			if (enter) {
				// finding common followers who unfollowed

					int check ;
					for (int i = 0; i < esCommonFollower.size(); i++) {
						
						check = followersWhoCommon.contains(esCommonFollower.get(i)) ? 1 : 0;
						if (check == 0) {
							commonUnFollowed.add(esCommonFollower.get(i).toString());						}
					}
					
					//finding common followers who unfollowed
					for (int i = 0; i < esNonCommonFollower.size(); i++) {
						
						check = followersNonCommon.contains(esNonCommonFollower.get(i)) ? 1 : 0;
						if (check == 0) {
							nonCommonUnFollowed.add(esNonCommonFollower.get(i).toString());						}
					}
				
			}
			
			// deleting those who Unfolloed me in mutualRelation type
			
		if (commonUnFollowed.size() >= 1) {
				
				// deleting from mutualRelation
				deleteFromES(indexEncription,credentialScreeName+influncerScreenName+"commonfollowers", commonUnFollowed);
		}
	
		// getting those who Unfolloed me in nonCommonrelation and deleting
		
	if (nonCommonUnFollowed.size() >= 1) {
			// deleting from mutualRelation
			deleteFromES(indexEncription,credentialScreeName+influncerScreenName+"noncommonfollowers", nonCommonUnFollowed);
	}
	
			relationIds.put("commonRelation",followersWhoCommon);
			relationIds.put("nonCommonRelation",followersNonCommon);
			
			Collections.sort(followersNonCommon.subList(0, followersNonCommon.size()));
			Collections.sort(followersWhoCommon.subList(0, followersWhoCommon.size()));
			
			Map<String, Object> updatefollowerRelation = new HashMap<String, Object>();
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"FollowerRelation",followersWhoCommon);
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"NonFollowerRelation",followersNonCommon);
			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			client.prepareUpdate(indexEncription,"user",userId).setDoc(updatefollowerRelation).setUpsert(updatefollowerRelation).setRefresh(true).execute().actionGet();
			client.close();
		
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		return relationIds;
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
	
	 public String[] getArrayIds(ArrayList<String> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
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
		Map<String, Object> responseMap = new HashMap<String, Object>();
		int keywordsIndex = 0;
		int credentialsIndex = 0;
		int emptyResult=0;
		//making index type date wise so we can easily delete data older than 3 days
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		String type = formatter.format(date);
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String keywordsJson = (routingContext.request().getParam("keywords") == null) ? "['cricket', 'football']": routingContext.request().getParam("keywords");
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "[]" : routingContext.request().getParam("credentials");
		long start = System.currentTimeMillis();
		boolean gotResult = true;
		try {
			
			String[] keywords = mapper.readValue(keywordsJson, String[].class);
			TypeReference<ArrayList<HashMap<String, Object>>> typeRef = new TypeReference<ArrayList<HashMap<String, Object>>>() {};
			ArrayList<HashMap<String, Object>> credentials = mapper.readValue(credentialsJson, typeRef);
			ArrayList<Map<String, Object>> tweets = null;
			if (keywords.length == 0 || credentials.size() == 0 || indexEncription.isEmpty()) {
				response = "correctly pass keywords or credentials ";
				log.warn("correctly pass keywords or credentials");
			} else {
				
				while (keywordsIndex < keywords.length) {
					System.err.println("user "+indexEncription+" credentialsIndex "+credentialsIndex +" keywordsIndex "+keywordsIndex);
					if (credentialsIndex > credentials.size() - 1)
						credentialsIndex = 0;

					Map<String, Object> credentialsMap = credentials.get(credentialsIndex);
					
					tweets = this.searchTweets(this.getTwitterInstance((String) credentialsMap.get("consumerKey"),(String)credentialsMap.get("consumerSecret"),(String) credentialsMap.get("accessToken"),(String) credentialsMap
					.get("accessTokenSecret")),	keywords[keywordsIndex]);
//					log.info("time taken for "+(keywords[keywordsIndex])+ " twitter4j to fetch Tweets "+ (System.currentTimeMillis()-start)+"tweets size "+tweets.size());
//					System.err.println("before keyword "+keywordsIndex +" credentials index "+credentialsIndex);
					if (tweets.size() == 0) {
						keywordsIndex--;
						emptyResult++;
					}
					
			if (tweets.size()==1) {
//				System.err.println("decrementing ctredentials index ");
				credentialsIndex--;
				tweets.remove(0);	
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
						emptyResult = 0;
					}
					keywordsIndex++;
					credentialsIndex++;
					
//					log.info("time taken for "+keywords[keywordsIndex]+ " to index in Es "+ (System.currentTimeMillis()-start)+"tweets size "+tweets.size());
					if ( emptyResult == credentials.size()) {
						gotResult = false;
//							break;
						System.err.println("Sleeping for 15 minutes ...!");
						Thread.sleep(904000);
						System.err.println("wakeUp from sleep...!");
						emptyResult = 0;
					}
				}
				
			}
			if (gotResult) {
				responseMap.put("status", "true");
				responseMap.put("size",tweets.size());
			}
			else {
				responseMap.put("status","false");
			}
			
			log.info("time taken total for Index Tweets "+ (System.currentTimeMillis()-start));
			response = mapper.writeValueAsString(responseMap);
			
		} catch (Exception ex) {
			response = 	"{\"status\" : \"error\", \"msg\" :" + ex.getMessage()+ "}";
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

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
	
	
	/**
	 * route for retweets
	 * 
	 * @param routingContext
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
		public void retweetBlocking(RoutingContext routingContext) {

		String response="";
		ObjectMapper mapper = new ObjectMapper();
		String tweetsId = (routingContext.request().getParam("tweetsId") == null) ? "[]": routingContext.request().getParam("tweetsId");
		String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		try {
			TypeReference<ArrayList<Long>> typeRef = new TypeReference<ArrayList<Long>>() {};
			ArrayList<Long> tweetsIdsList = mapper.readValue(tweetsId, typeRef);

			TypeReference<HashMap<String, Object>> credentialsTypeReference = new TypeReference<HashMap<String, Object>>() {};
			HashMap<String, Object> credentials = mapper.readValue(credentialsjson, credentialsTypeReference);

			ArrayList<Long> retweetIds = twitter4jApi.retweet(this.getTwitterInstance(credentials.get("consumerKey").toString(), credentials.get("consumerSecret").toString(), credentials.get("accessToken").toString(), credentials.get("accessTokenSecret").toString()), tweetsIdsList);
			response ="{\"status\" :" + mapper.writeValueAsString(retweetIds)
					+ "}";
		

		} catch (Exception ex) {
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

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
		String userIds = (routingContext.request().getParam("screenNames") == null) ? "": routingContext.request().getParam("screenNames");
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
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

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
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

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
		String friendsListJson = (routingContext.request().getParam("screenNames") == null) ? "[]" : routingContext.request().getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {
			};
			HashMap<String, String> credentials = mapper.readValue(	credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> friendsListType = new TypeReference<ArrayList<String>>() {	};
			ArrayList<String> friendsList = mapper.readValue(friendsListJson,friendsListType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.muteUser(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),credentials.get("accessToken"),credentials.get("accessTokenSecret")), friendsList);
			responseMap.put("muted", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

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
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

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
			String response;
			ObjectMapper mapper = new ObjectMapper();
			
			String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
			String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");

			
			try {
	
				TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String,Object>>() {	};
				HashMap<String, Object> credentials = mapper.readValue(credentialsjson, typeRef);
				ArrayList<Map<String, Object>> documents=null;
				documents = this.userInfo(this.getTwitterInstance(credentials.get("consumerKey").toString(),credentials.get("consumerSecret").toString(),credentials.get("accessToken").toString(),credentials.get("accessTokenSecret").toString()),screenName);
				
				

				if (documents.size()!=0) {
				responseMap.put("userInfo", documents);
				responseMap.put("size", documents.size());
				
			}
			
			response = mapper.writeValueAsString(responseMap);

			} catch (Exception e) {
				log.error(e.getMessage());
//				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
						+ "}";
			}

			routingContext.response().end(response);

		}

		/**
		 * use to get just userInfo not indexing in elasticsearch
		 * 
		 * @param routingContext
		 */
		public void indexUserInfoRouteBlocking(RoutingContext routingContext) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			String response;
			ObjectMapper mapper = new ObjectMapper();
			
			String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
			String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");
			
			try {
	
				TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String,Object>>() {	};
				HashMap<String, Object> credentials = mapper.readValue(credentialsjson, typeRef);
				ArrayList<Map<String, Object>> documents=null;
				documents = this.twitter4jApi.getStickyInfo(this.getTwitterInstance(credentials.get("consumerKey").toString(),credentials.get("consumerSecret").toString(),credentials.get("accessToken").toString(),credentials.get("accessTokenSecret").toString()),screenName);

				if (documents.size()!=0) {	
				responseMap.put("documents", documents);
				responseMap.put("size", documents.size());
				
			}
			
			response = mapper.writeValueAsString(responseMap);

			} catch (Exception e) {
				log.error(e.getMessage());
//				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
						+ "}";
			}

			routingContext.response().end(response);

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
		 * use to get tweets
		 * 
		 * @param twitter
		 * @param query
		 * @return list of tweets
		 * @throws TwitterException
		 * @throws Exception
		 */

		public ArrayList<Map<String, Object>> userInfo(Twitter twitter,String screenName)throws Exception {
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
		ArrayList<Map<String, Object>> tweets = twitter4jApi.search(twitter,keyword);
		return tweets;

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
