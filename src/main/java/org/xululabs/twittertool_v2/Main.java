package org.xululabs.twittertool_v2;


import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger; 


public class Main {
	
	private static Logger log = LogManager.getRootLogger();

	public static void main(String[] args) {

	
		log.info("Initializing twittertool-v2");
		
			Vertx vertx = Vertx.vertx();
//			vertx.deployVerticle(new DeployServer());

			vertx.deployVerticle(new SearchingServer());
			vertx.deployVerticle(new IndexingServer());
			
		
		
	}
	
}
