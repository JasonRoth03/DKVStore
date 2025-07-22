package com.JasonRoth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashingManager {
    final SortedMap<Long, String> ring = new TreeMap<>();
    private final int numberOfReplicas; //number of virtual nodes

    public ConsistentHashingManager(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    public synchronized void updateNodes(Collection<String> nodes){
        ring.clear();
        for(String node : nodes){
            addNode(node);
        }
    }

    private void addNode(String node){
        for(int i = 0; i < numberOfReplicas; i++){
            ring.put(hash(node + i), node);
        }
    }

    public synchronized String getNodeForKey(String key){
        if(ring.isEmpty()){
            System.out.println("Ring is empty");
            return null;
        }
        long hash = hash(key);
        if(!ring.containsKey(hash)){
            //find the next node on the ring clockwise
            SortedMap<Long, String> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    public synchronized List<String> getNodesForKey(String key, int replicas){
        System.out.println("Getting nodes for key: " + key);
        if(ring.isEmpty()){
            System.out.println("Ring is empty");
            return null;
        }

        List<String> nodes = new ArrayList<>();
        Set<String> uniqueNodes = new LinkedHashSet<>();
        long hash = hash(key);

        Iterator<Long> it = ring.tailMap(hash).keySet().iterator();
        while(uniqueNodes.size() < replicas && uniqueNodes.size() < ring.values().stream().distinct().count()){
            //if we run out of nodes in the tail map, then wrap around
            if(!it.hasNext()){
                it = ring.keySet().iterator();
            }
            uniqueNodes.add(ring.get(it.next()));
        }

        nodes.addAll(uniqueNodes);
        return nodes;
    }

    //using a simple long hash
    //consider using more robust hash function
    private long hash(String key){
        try{
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes());
            long h = 0;
            for(int i = 0; i < 4; i++){
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not found", e);
        }
    }
}
