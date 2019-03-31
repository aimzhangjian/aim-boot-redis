package com.aim.redis.study;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * description
 *
 * @author aim 2019/03/20 18:34
 */
@Component
public class Chapter02 {
    @Autowired
    RedisTemplate<String, String> redisTemplate;
    @Autowired
    private ValueOperations<String, String> valueOperations; //简单k-v操作
    @Autowired
    private ListOperations<String, String> listOperations; //list类型数据操作
    @Autowired
    private SetOperations<String, String> setOperations; //set类型数据操作
    @Autowired
    private HashOperations<String, String, String> hashOperations; //map类型数据操作
    @Autowired
    private ZSetOperations<String, String> zSetOperations; //zset类型数据操作

    @Autowired
    private BoundValueOperations<String, String> boundValueOperations; //简单k-v操作
    @Autowired
    private BoundListOperations<String, String> boundListOperations; //list类型数据操作
    @Autowired
    private BoundSetOperations<String, String> boundSetOperations; //set类型数据操作
    @Autowired
    private BoundHashOperations<String, String, String> boundHashOperations; //map类型数据操作
    @Autowired
    private BoundZSetOperations<String, String> boundZSetOperations; //zset类型数据操作


    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;


    public String checkToken(String token) {
        return hashOperations.get("login:", token);
    }

    public void updateToken(String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        hashOperations.put("login:", token, user);
        zSetOperations.add("recent:", token, timestamp);
        if (item != null) {
            zSetOperations.add("viewed:" + token, item, timestamp);
            zSetOperations.removeRange("viewed:" + token, 0, -26);
            zSetOperations.incrementScore("viewed:", item, -1);
        }
    }


    public void addToCart(String session, String item, int count) {
        if (count <= 0) {
            hashOperations.delete("cart:" + session, item);
        } else {
            hashOperations.put("cart:" + session, item, String.valueOf(count));
        }
    }


    public class CleanSessionsThread extends Thread {
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit) {
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            while (!quit) {
                long size = zSetOperations.zCard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> tokenSet = zSetOperations.range("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);
                List<String> sessionKeys = new ArrayList<>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }

                redisTemplate.delete(sessionKeys);
                hashOperations.delete("login:", tokens);
                zSetOperations.remove("recent:", tokens);
            }
        }
    }

    public class CleanFullSessionsThread extends Thread {
        private boolean quit;
        private int limit;

        public CleanFullSessionsThread(int limit) {
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            while (!quit) {
                long size = zSetOperations.zCard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> sessionSet = zSetOperations.range("recent:", 0, endIndex - 1);
                String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);
                ArrayList<String> sessionKeys = new ArrayList<>();
                for(String sess : sessions){
                    sessionKeys.add("viewed:" + sess);
                    sessionKeys.add("cart:" + sess);
                }
                redisTemplate.delete(sessionKeys);
                hashOperations.delete("login:", sessions);
                zSetOperations.remove("recent:", sessions);
            }
        }
    }
}
