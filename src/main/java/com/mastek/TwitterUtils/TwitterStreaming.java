package com.mastek.TwitterUtils;

import java.io.IOException;

import com.mastek.mapr.stream.MapRStreamProducer;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterStreaming {
	static MapRStreamProducer producer = new MapRStreamProducer();
	
	public static void main(String[] args) throws TwitterException, IOException {
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				System.out.println("["+ status.getUser().getName() + "]" + status.getText());
				
				producer.produce(status.getUser().getName(), status.getText());
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}
		};

		System.out.println("Starting");
		
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(listener);

//		twitterStream.sample();

//		twitterStream.filter("language");
		
		FilterQuery tweetFilterQuery = new FilterQuery(); 
//		tweetFilterQuery.track(new String[]{"Bieber", "Teletubbies"});
		tweetFilterQuery.locations(new double[][]{new double[]{-180,-85}, new double[]{180,85}}); 
////		See https://dev.twitter.com/docs/streaming-apis/parameters#locations for proper location doc. 
////		Note that not all tweets have location metadata set.
		tweetFilterQuery.language(new String[]{"en"});
		twitterStream.filter(tweetFilterQuery);
	}
}
