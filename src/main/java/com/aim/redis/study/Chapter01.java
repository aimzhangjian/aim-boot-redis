package com.aim.redis.study;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.stereotype.Component;

import java.util.*;
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

    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;


    public String postArticle(String user, String title, String link) {
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

    public void articleVote(String user, String article) {
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (zSetOperations.score("time:", article) < cutoff) {
            return;
        }
        String articleId = article.substring(article.indexOf(":") + 1);
        if (setOperations.add("voted:" + articleId, user) == 1) {
            zSetOperations.incrementScore("score:", article, VOTE_SCORE);
        }
        hashOperations.increment(article, "votes", 1);
    }

    public List<Map<String, String>> getArticles(int page) {
        return getArticles(page, "score:");
    }

    public List<Map<String, String>> getArticles(int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;
        Set<String> ids = zSetOperations.reverseRange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids) {
            Map<String, String> articleData = (Map<String, String>) hashOperations.entries(id);
            articleData.put("id", id);
            articles.add(articleData);
        }
        return articles;
    }

    public void addGroups(String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            setOperations.add("group:" + group, article);
        }
    }

    public List<Map<String, String>> getGroupArticles(String group, int page) {
        return getGroupArticles(group, page, "score:");
    }

    public List<Map<String, String>> getGroupArticles(String group, int page, String order) {
        String key = order + group;
        List arrayList = new ArrayList();
        arrayList.add(order);
        if (!redisTemplate.hasKey(key)) {
            zSetOperations.intersectAndStore("group:" + group, arrayList, key, RedisZSetCommands.Aggregate.MAX);
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }
        return getArticles(page, key);
    }
}
