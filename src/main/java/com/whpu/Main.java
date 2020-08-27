package com.whpu;

import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author cc
 * @create 2020-08-07-17:22
 */
public class Main {
    public static void main(String[] args) {
        int[][] matrix = {{1,4,7,11,15},{2,5,8,12,19},{3,6,9,16,22},{10,13,14,17,24},{18,21,23,26,30},{18,21,23,26,30}};

        Solution s = new Solution();
        System.out.println(s.findMedianSortedArrays(new int[]{1,4,7,11,15},new int[]{2}));
    }
}
class Solution {
    public double findMedianSortedArrays(int[] nums1, int[] nums2) {
        List<Integer> arr1 = Arrays.asList(ArrayUtils.toObject(nums1));
        for (int i = 0; i < nums2.length; i++) {

            arr1.add(Arrays.binarySearch(nums1,nums2[i]),nums2[i]);
        }
        if (arr1.size()%2==1){
            return new Double(arr1.get(arr1.size() /2 + 1));
        }else {
            return new Double(arr1.get(arr1.size() /2)+arr1.get(arr1.size() /2+1)/2);

        }
    }
}