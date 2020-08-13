package com.whpu;

/**
 * @author cc
 * @create 2020-08-07-17:22
 */
public class Main {
    public static void main(String[] args) {
        int[][] matrix = {{1,4,7,11,15},{2,5,8,12,19},{3,6,9,16,22},{10,13,14,17,24},{18,21,23,26,30},{18,21,23,26,30}};

        Solution s = new Solution();
        System.out.println(s.findNumberIn2DArray(matrix,5));
    }
}
class Solution {
    public boolean findNumberIn2DArray(int[][] matrix, int target) {

//        int[] temp = new int[matrix[0].length];//matrix.length获取行数     matrix[0].length获取列数
        //暴力
        int n = matrix.length;
        int m = matrix[0].length;

        System.out.println(n+" "+m+" "+matrix[5][4]);
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {

                while (matrix[i][j]==target){

                    return true;
                }
            }

        }
        return false;
    }
}