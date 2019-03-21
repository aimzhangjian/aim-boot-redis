package com.aim.redis.study;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * description
 *
 * @author aim 2019/03/20 18:34
 */
@Component
public class Chapter01 {
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


    public String postArticle(String user, String title, String link){
        String articleId = String.valueOf(valueOperations.increment("article:"));
        String voted = "voted:" + articleId;
        setOperations.add(voted, user);
        redisTemplate.expire(voted, ONE_WEEK_IN_SECONDS, TimeUnit.SECONDS);

        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        Map<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        hashOperations.putAll(article, articleData);
        zSetOperations.add("score:", article, now + VOTE_SCORE);
        zSetOperations.add("time:", article, now);
        return articleId;
    }
}
