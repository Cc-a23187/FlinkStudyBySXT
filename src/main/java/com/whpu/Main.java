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
        System.out.println(s.findMedianSortedArrays(new int[]{1,3},new int[]{}));
    }
}
class Solution {
    public double findMedianSortedArrays(int[] nums1, int[] nums2) {
        int[] sum = new int[nums1.length+nums2.length];
        int j = 0;
        if (nums1.length==0&&nums1.length==0)
            System.out.println(sum.toString());
        else if (nums1.length==0) {
            sum=nums2;
        }
        else if (nums2.length==0) {
            sum=nums1;
        }
        else{
            for (int i = 0; i < nums1.length; i++) {
                sum[i]=nums1[i];

            }
            for (int i = nums1.length; i < sum.length; i++) {
                sum[i]=nums2[i-nums1.length];
            }
            Arrays.sort(sum);
        }


        if (sum.length%2==1){
            return new Double(sum[sum.length /2 + 1]);
        }else {
            return new Double((sum[sum.length /2]+sum[sum.length /2 + 1])/2);

        }
  /*      List<Integer> arr1 = Arrays.asList(ArrayUtils.toObject(nums1));
        for (int i = 0; i < nums2.length; i++) {

            arr1.add(Arrays.binarySearch(nums1,nums2[i]),nums2[i]);
        }
        if (arr1.size()%2==1){
            return new Double(arr1.get(arr1.size() /2 + 1));
        }else {
            return new Double(arr1.get(arr1.size() /2)+arr1.get(arr1.size() /2+1)/2);

        }*/
    }
}