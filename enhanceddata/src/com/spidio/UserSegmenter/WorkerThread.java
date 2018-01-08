package com.spidio.UserSegmenter;



import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import org.elasticsearch.search.SearchHit;















import com.spidio.dataModel.DeviceObject;
import com.spidio.dataModel.LocationObject;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

	public class WorkerThread implements Runnable {

		SearchHit hit;
		Client client;
		BulkProcessor bulkProcessor;
		String keywords = null;
		String description = null;
		String[] searchkeyword = null;
		String[] finalsearchkeyword = null;
		SearchHit[] matchingsegmentrecords = null;
		String category = null;
		String subcategory = null;
		DeviceObject deviceProperties = null;
		LocationObject locationProperties = null;

		// SearchHit[] results =
		// IndexCategoriesData.searchEntireUserData(client,
		// "dmpuserdatabase","core2");

		// System.setOut(new PrintStream(new BufferedOutputStream(new
		// FileOutputStream("log.txt"))));

		// Scans quarterly data of the day, enhances its data points and store
		// it in new index
		String refurl;
		String refcurrent;
		String clickedurl;

		//String reforiginal = (String) result.get("reforiginal");
	   // String refcuroriginal = (String) result.get("refcurrentoriginal");
	   // String clickedurloriginal = (String) result.get("clickurloriginal");
		String localstorageId;
		String page_title;
		String documentId;
		String ip;
		String browser;
		String userAgent;
		String channel_name;
		String subcategorydata;
		String localstorageid;
		String fingerprint_id;
		String cookie_id;
		String session_id;
		String master_id;
		String plugin_data;
		String audience_segment;
		String request_time;
		String system_os;
		String date;




		String brand_name = null;
		String browserversion = null;
		String model_name = null;
		String device_os = null;
		String browser1 = null;
		String picture_gif = null;
		String picture_jpg = null;
		String picture_png = null;
		String gif_animated = null;
		String streaming_video = null;
	    String streaming_3gpp = null;
		String streaming_mp4 = null;
		String streaming_mov = null;
		String colors = null;
		String dual_orientation = null;
		String ux_full_desktop = null;
		String city = null;
		String state = null;
		String org = null;
		String isp = null;
		String gender[] = { "male", "female" };
		String agegroup[] = { "13_17", "18_24", "25_34", "35_44",
				"45_54", "55_64", "65_more" };
		String agegroup1[] = {"25_34","35_44","45_54"};
		String gender1 = "male";
		String incomelevel[] = {"high","medium","medium","medium","low"};
		String incomelevel1[] = {"high","high","medium","medium","medium","low"};
		String subcategory_entertainment[] = {
				"_entertainment_bollywood", "_entertainment_music",
				"_entertainment_tv_shows", "_entertainment_games" };
		String subcategory_lifestyle[] = { "_lifestyle_food",
				"_lifestyle_health_fitness", "_lifestyle_fashion" };
		String subcategory_news[] = { "_news_india_news",
				"_news_world_news" };
		String subcategory_education[] = { "_education_mba",
				"_education_mca", "_education_btech",
				"_education_online_courses" };
		String subcategory_technology[] = { "_technology_mobile",
				"_technology_computing", "_technology_apps",
				"_technology_gadgets" };
		String subcategory_business[] = { "_business_india_business",
				"_business_international_business",
				"_business_markets", "_business_mf_simplified",
				"_business_startups" };
		String QuarterValue = null;


		Map<String, Object> result0;
		Map<String, Object> result;
		Map<String, Object> result1;
		Map<String, Object> result2;

		Integer delta=0;

	    public WorkerThread(SearchHit hit, Client client){
	        this.hit = hit;
	        this.client = client;
	        this.bulkProcessor = BulkProcessorModule.GetInstance(client);

	    }

	    @Override
	    public void run() {
	    	//Handle the hit...

	    	String srcIP = "";
	    	String ES_ID = this.hit.getId();
	    	String ES_INDEX = "enhanceduserdatabeta1";
	    	try {





					System.out.println("------------------------------");
					result = hit.getSource();
					refurl = (String) result.get("referrer");
					refcurrent = (String) result.get("refcurrent");
					clickedurl =  (String) result.get("clickedurl");
					System.out.println(refurl);
					//String reforiginal = (String) result.get("reforiginal");
				   // String refcuroriginal = (String) result.get("refcurrentoriginal");
				   // String clickedurloriginal = (String) result.get("clickurloriginal");
					localstorageId = (String)result.get("localStorageid");
					page_title = (String) result.get("page_title");
					documentId = (String) result.get("_id");
					ip = (String) result.get("ip");
					browser = (String) result.get("browser_name");
					userAgent = (String) result.get("br_user_agent");
					channel_name = (String) result.get("channel_name");
					subcategorydata = (String) result.get("subcategory");
					localstorageid = (String) result.get("localStorageid");
					fingerprint_id =(String) result.get("fingerprint_id");
					cookie_id = (String) result.get("cookie_id");
					session_id = (String)result.get("session_id");
					master_id = (String) result.get("mastercookie_id");
					plugin_data = (String) result.get("plugin");
				    audience_segment = (String) result.get("audience_segment");
				    request_time = (String) result.get("request_time");
					system_os = (String) result.get("system_os");

					 system_os = system_os.replace(" ", "_").replace("-", "_");

					 date = request_time.substring(0, 10);
					 System.out.println(userAgent);
					 brand_name = null;


			        QuarterValue = null;
					int hour = Integer.parseInt(request_time.substring(11, 13)
							.trim());
					System.out.println(request_time);
					System.out.println(hour);
					// This Quartely data of the day is aggregated later on and used
					// for time of the day reports

					if (page_title != null) {

						page_title = page_title.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						System.out.println(page_title);
					}

					if (refurl != null) {

						result.put("reforiginal",refurl);
						refurl = refurl.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						result.put("referrer", refurl);

						System.out.println(refurl);
					}


					if (refcurrent != null) {


						result.put("refcurrentoriginal",refcurrent);
						refcurrent = refcurrent.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						result.put("refcurrent", refcurrent);

						System.out.println(refcurrent);
					}



					if (clickedurl != null) {

						result.put("clickurloriginal",clickedurl);
						clickedurl = clickedurl.replace("-", "_").replace(".", "_")
								.replace(":", "_").replace(" ", "_")
								.replace("/", "_").replace("//", "_");
						result.put("clickedurl", clickedurl);

						System.out.println(clickedurl);
					}


					 Random random = new Random();






					if (hour < 4)
						QuarterValue = "Quarter1";

					if (hour >= 4 && hour < 8)
						QuarterValue = "Quarter2";

					if (hour >= 8 && hour < 12)
						QuarterValue = "Quarter3";

					if (hour >= 12 && hour < 16)
						QuarterValue = "Quarter4";

					if (hour >= 16 && hour < 20)
						QuarterValue = "Quarter5";

					if (hour >= 20 && hour < 24)
						QuarterValue = "Quarter6";

					System.out.println(QuarterValue);

					int idx;
					result.put("date", date);
					deviceProperties = ProcessDeviceData
							.getDeviceDetails(userAgent);

					if (deviceProperties != null) {
						brand_name = deviceProperties.getBrandName();
						if (brand_name != null)
							brand_name = deviceProperties.getBrandName()
									.replace(" ", "_").replace("-", "_");

						model_name = deviceProperties.getModel_name();

						browserversion = deviceProperties.getBrowserversion();


						if(browserversion != null && !browserversion.isEmpty())
						result.put("browserversion", browserversion);

						String release_date = deviceProperties.getRelease_date();

						if (model_name != null)
							model_name = deviceProperties.getModel_name().replace("-", "_").replace(":", "_").replace(" ", "_")
						  			.replace("/", "_").replace("//", "_").replace("+", "_").replace(")","_").replace("(", "_");

						if (release_date != null)
							release_date = deviceProperties.getRelease_date()
									.replace(" ", "_").replace("-", "_");

						model_name = brand_name + "_" + model_name + "_"
								+ release_date;


						browser1 = deviceProperties.getBrowser();

						browser1 =  browser1.replace("-", "_").replace(":", "_").replace(" ", "_")
					  			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_");

						browser =  browser.replace("-", "_").replace(":", "_").replace(" ", "_")
					  			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_");

						if(browser.trim().toLowerCase().contains("handheld")==true)
						    result.put("browser_name", browser1);
						else
						    result.put("browser_name", browser);


						device_os = deviceProperties.getDeviceOs();
						String device_os_version = deviceProperties
								.getDevice_os_version();

						if (device_os != null)
							device_os = deviceProperties.getDeviceOs()
									.replace(" ", "_").replace("-", "_");

						if (device_os_version != null)
							device_os_version = deviceProperties
									.getDevice_os_version().replace(" ", "_")
									.replace("-", "_");

						device_os = device_os + "_" + device_os_version;
					}

					if (channel_name != null)
						channel_name = channel_name.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

					if (fingerprint_id != null)
						fingerprint_id = fingerprint_id.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

					if (cookie_id != null)
						cookie_id = cookie_id.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");



					if (session_id != null)
						session_id = session_id.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");





					if (localstorageid != null)
						localstorageid = localstorageid.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");

					if (plugin_data != null)
						plugin_data = plugin_data.replace("-", "_")
								.replace(".", "_").replace(" ", "_")
								.replace("#", "_");


					if(master_id != null)
						master_id = master_id.replace("-", "_")
						.replace(".", "_").replace(" ", "_")
						.replace("#", "_");

					if (browser != null)
						browser = browser.replace(" ", "_").replace("-", "_");

					// String userAgent =
					// "Mozilla/5.0 (Linux; Android 4.4.2; SGH-I257M Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.89 Mobile Safari/537.36";
					if (audience_segment != null) {
						audience_segment = audience_segment.replace("-", "_")
								.replace(" ", "_").replace("#", "").toLowerCase()
								.replace("technologynology", "technology")
								.replace("food&beverages", "food_beverages")
								.replace("life_style", "lifestyle")
								.replace("india_news", "news")
								.replace("accessories", "clothes")
								.replace("indianexpress.com", "news")
								.replace("wolifestyle", "women_lifestyle");

					}

					// deviceProperties =
					// ProcessDeviceData.getDeviceDetails(userAgent);
					// String channelName = (String)result.get("channel_name");

					// deviceProperties =
					// ProcessDeviceData.getDeviceDetails(userAgent);

					if (channel_name != null) {
						/*
						 * if (channel_name.contains("Mumbai")) { result.put("city",
						 * "Mumbai"); result.put("country", "India");
						 * result.put("latitude_longitude", "Mumbai_19_72.8");
						 * result.put("postalcode", "Mumbai_400099");
						 *
						 * } else {
						 */
						if (ip != null) {
							locationProperties = ProcessIPAddress.getIPDetails(ip);
							if (locationProperties != null) {
								city = locationProperties.getCity();
								if (city != null) {
									city = locationProperties.getCity()
											.replace(" ", "_").replace("-", "_");
									result.put("city", city);
									result.put("country",
											locationProperties.getCountry().replace(" ", "_").replace("-", "_"));
									if (locationProperties.getLatittude() != null)
										result.put("latitude_longitude", city
												+ "_"
												+ locationProperties.getLatittude()
														.toString()
												+ "_"
												+ locationProperties.getLongitude()
														.toString());
									if (locationProperties.getPostalCode() != null)
										result.put(
												"postalcode",
												city
														+ "_"
														+ locationProperties
																.getPostalCode());


								  if(locationProperties.getState()!=null)
								  {
									  state = locationProperties.getState().replace(" ", "_").replace("-", "_");
									  result.put("state",state);
								  }
								}
							}
						}
						// }
					}

					if (ip != null) {

						org = ProcessOrganisationData.getOrgDetails(ip);
						if (org != null)
							org = org.replace("-", "_").replace(":", "_").replace(" ", "_")
						  			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_").replace("&", "_");

						isp = ProcessISPData.getISPDetails(ip);
						if (isp != null)
							isp = isp.replace("-", "_").replace(":", "_").replace(" ", "_")
			    			.replace("/", "_").replace("//", "_").replace(")","_").replace("(", "_").replace("+", "_").replace("&", "_");

					}

					result.put("brandName", brand_name);
					result.put("modelName", model_name);
					if (device_os.contains("Desktop") == false)
						result.put("system_os", device_os);
					else
						result.put("system_os", system_os);

					result.put("channel_name", channel_name);


					if (cookie_id ==null || cookie_id.isEmpty())
			          {

			    		if (localstorageId !=null && !localstorageId.isEmpty()){
			    		cookie_id = localstorageId;
			        	cookie_id = cookie_id.replace("-", "_");
			    		}
			          }

					result.put("cookie_id", cookie_id);




					result.put("session_id", session_id);







					result.put("fingerprint_id", fingerprint_id);
					result.put("plugin",plugin_data);
					result.put("mastercookie_id", master_id);
					result.put("audience_segment", audience_segment);
					result.put("referrer", refurl);
					result.put("page_title", page_title);
					result.put("organisation", org);
					result.put("ISP", isp);
					result.put("QuarterValue", QuarterValue);
					idx = new Random().nextInt(gender.length);

					result.put("gender", gender[idx]);
					idx = new Random().nextInt(agegroup.length);
					int idx1 = new Random().nextInt(agegroup1.length);

					idx = new Random().nextInt(incomelevel.length);
					result.put("incomelevel",incomelevel[idx]);

	                int idx1a = new Random().nextInt(incomelevel1.length);

					if(channel_name.trim().toLowerCase().equals("cuberoot")){
					result.put("agegroup",agegroup1[idx1] );
	                result.put("gender", gender1);
	                result.put("incomelevel",incomelevel1[idx1a]);
					}
					else
					result.put("agegroup", agegroup[idx]);


	                if(brand_name.equals("OnePlus")){
	                	result.put("agegroup", "35_44");
	                    result.put("gender","male");
	                }

	                if(brand_name.equals("Samsung")){
	                	result.put("agegroup", "35_44");
	                    result.put("gender","male");
	                }



					if (audience_segment != null) {
						if (audience_segment.trim().toLowerCase().equals("entertainment")) {

							result.put("subcategory",
									"_entertainment_"+subcategorydata);
						}

						if (audience_segment.trim().toLowerCase().equals("news")) {

							result.put("subcategory","_news_"+subcategorydata);
						}

						if (audience_segment.trim().toLowerCase().equals("lifestyle")) {

							result.put("subcategory","_lifestyle_"+subcategorydata);
						}

	                    if (audience_segment.trim().toLowerCase().equals("sports")) {

							result.put("subcategory","_sports_"+subcategorydata);
						}


	                    if (audience_segment.trim().toLowerCase().equals("travel")) {

							result.put("subcategory","_travel_"+subcategorydata);
						}


					//	if (audience_segment.trim().toLowerCase().equals("education")) {

					//		result.put("subcategory", "_education_"+subcategorydata);
					//	}

						if (audience_segment.trim().toLowerCase().equals("technology")) {

							result.put("subcategory","_technology_"+subcategorydata);
						}

						if (audience_segment.trim().toLowerCase().equals("business")) {

							result.put("subcategory","_business_"+subcategorydata);
						}


	                    if (audience_segment.trim().toLowerCase().equals("foodbeverages")) {

							result.put("subcategory","_foodbeverages_"+subcategorydata);
						}


	                    if (audience_segment.trim().toLowerCase().equals("food_beverages")) {

	                    	result.put("audience_segment","foodbeverages");
	                    	result.put("subcategory","_foodbeverages_"+subcategorydata);
						}


	                    if(subcategorydata == null || subcategorydata.isEmpty() == true )
	                    {
	                    	result.put("subcategory","");


	                    }


					    if(channel_name.trim().toLowerCase().equals("cuberoot"))
						   result.put("subcategory","_business_startups");

					}

					if (deviceProperties != null) {
						result.put(
								"screen_properties",
								deviceProperties.getPhysical_screen_width()
										+ "_"
										+ deviceProperties
												.getPhysical_screen_height());
						result.put("resolution_properties",
								deviceProperties.getResolution_width() + "_"
										+ deviceProperties.getResolution_height());
						result.put("isWireless",
								deviceProperties.getWireless_device());


					    result.put("picture_gif", deviceProperties.getPicture_gif());
					    result.put("picture_jpg", deviceProperties.getPicture_jpg());
					    result.put("picture_png", deviceProperties.getPicture_png());
					    result.put("gif_animated", deviceProperties.getGif_animated());
					    result.put("streaming_video", deviceProperties.getStreaming_video()) ;
					    result.put("streaming_mp4", deviceProperties.getStreaming_mp4());
					    result.put("streaming_3gpp", deviceProperties.getStreaming_3gpp());
					  //  result.put("streaming_video", deviceProperties.getStreaming_video());
					  //  result.put("streaming_mp4", deviceProperties.getStreaming_mp4());
					    result.put("streaming_mov", deviceProperties.getStreaming_mov());
					    result.put("colors", deviceProperties.getColors());
					    result.put("dual_orientation", deviceProperties.getDual_orientation());
					    result.put("ux_full_desktop", deviceProperties.getUx_full_desktop());


					    if(deviceProperties.getWireless_device().equals("false"))
							result.put("device","Computer");

					    if(deviceProperties.getIs_tablet().equals("true"))
								result.put("device","Tablet");

					    if(deviceProperties.getIs_tablet().equals("false") && deviceProperties.getWireless_device().equals("true"))
					            result.put("device","Mobile");

					    /*
					    String mobilesId =  deviceProperties.getBrandName()+" "+deviceProperties.getModel_name()+" "+deviceProperties.getMarketing_name()+" "+deviceProperties.getRelease_date();

					    String mobileproperties = GetWurflData.get91mobilesData(mobilesId.trim());

					    String [] otherProperties = mobileproperties.split(":");

					        if( otherProperties[0]!= null)
						    otherProperties[0]= otherProperties[0].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[1]!= null)
						    otherProperties[1]= otherProperties[1].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[2]!= null)
						    otherProperties[2]=	otherProperties[2].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[3]!= null)
						    otherProperties[3]= otherProperties[3].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[4]!= null)
						    otherProperties[4]= otherProperties[4].replace(" ", "_").replace("-", "_").replace(",", "_");
						    if( otherProperties[5]!= null)
						    otherProperties[5]= otherProperties[5].replace(" ", "_").replace("-", "_").replace(",", "_");




						result.put("price_range",otherProperties[0]);
						result.put("processor",otherProperties[1]);
					    result.put("display",otherProperties[2]);
					    result.put("primarycamera",otherProperties[3]);
					    result.put("battery",otherProperties[4]);
					    result.put("frontcamera",otherProperties[5]);
					    */
					}








				     bulkProcessor.add(new IndexRequest().index(ES_INDEX).type(this.hit.getType()).source(result)
					        );





	    	}

			 catch (Exception e) {

				bulkProcessor.add(new IndexRequest().index(ES_INDEX).type(this.hit.getType()).source(result)
					        );
				e.printStackTrace();


			}

	    	  finally {
	    		  bulkProcessor.close();
	          }

        }




}
