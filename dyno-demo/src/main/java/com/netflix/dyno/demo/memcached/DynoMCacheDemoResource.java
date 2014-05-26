package com.netflix.dyno.demo.memcached;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.demo.DynoDataBackfill;

@Path("/dyno/demo/memcached")
public class DynoMCacheDemoResource {

	private static final Logger Logger = LoggerFactory.getLogger(DynoMCacheDemoResource.class);

	public DynoMCacheDemoResource() {
	}

	@Path("/dataFill/start")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String startDataFill() throws Exception {

		Logger.info("Starting dyno data fill"); 
		try {
			new DynoDataBackfill(DynoMCacheDriver.getInstance().getDynoClient()).backfill();
			return "data fill done!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting datafill", e);
			return "dataFill failed!";
		}
	}
	
	
	// ALL DYNO RESOURCES
	
	// ALL DYNO RESOURCES
	@Path("/init")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoInit() throws Exception {

		DynoMCacheDriver.getInstance().init();
		return "Dyno client inited!" + "\n";
	}
	
	@Path("/start")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStart() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoMCacheDriver.getInstance().start();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}
		
	@Path("/startReads")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStartReads() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoMCacheDriver.getInstance().startReads();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}
		
	@Path("/startWrites")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStartWrites() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoMCacheDriver.getInstance().startWrites();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}
		
	@Path("/stop")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStop() throws Exception {

		Logger.info("Stopping dyno demo test"); 
		try {
			DynoMCacheDriver.getInstance().stop();
			return "Dyno test stopped!" + "\n";
		} catch (Exception e) {
			Logger.error("Error stopping dyno test", e);
			return "dyno stop failed! " + e.getMessage();
		}
	}

	
	@Path("/readSingle/{key}")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String readSingle(@PathParam("key") String key) throws Exception {

		Logger.info("Stopping dyno demo test"); 
		try {
			System.out.println("Key: " + key);
			return "\n" + DynoMCacheDriver.getInstance().readSingle(key) + "\n";
		} catch (Exception e) {
			Logger.error("Error stopping dyno test", e);
			return "dyno stop failed! " + e.getMessage();
		}
	}
	
	@Path("/writeSingle/{key}/{val}")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String writeSingle(@PathParam("key") String key, @PathParam("val") String value) throws Exception {

		Logger.info("Stopping dyno demo test"); 
		try {
			System.out.println("Key: " + key + ", value: " + value);
			return "\n" + DynoMCacheDriver.getInstance().writeSingle(key, value) + "\n";
		} catch (Exception e) {
			Logger.error("Error stopping dyno test", e);
			return "dyno stop failed! " + e.getMessage();
		}
	}
	
	@Path("/status")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStatus() throws Exception {

		Logger.info("Stating dyno data fill"); 
		try {
			return DynoMCacheDriver.getInstance().getStatus();
		} catch (Exception e) {
			Logger.error("Error getting dyno status", e);
			return "dyno status failed! " + e.getMessage();
		}
	}
	
	@Path("/removeOneHost")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String removeOneHost() throws Exception {
		return "Not implemented!!" + "\n";
	}
}
