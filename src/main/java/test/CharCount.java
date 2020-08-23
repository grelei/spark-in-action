package test;

public class CharCount {
    /*
     * 依次顺序遍历
     * @param srcStr
     * @param searchStr
     */
    public static void strCountWhitIterator(String srcStr,String searchStr){
        if(srcStr.length() > 0 && searchStr.length() > 0){
            int srcStrLength = srcStr.length();
            int searchStrLength = searchStr.length();
            int count = 0;
            if(srcStrLength > searchStrLength){
                int stopIndex = srcStrLength - searchStrLength;
                long starTime = System.currentTimeMillis();
                //第一种遍历方式，顺序遍历
                for(int i= 0 ;i<=srcStrLength;i++){
                    if(i>stopIndex){
                        System.out.println("顺序遍历统计方式出现次数为："+count);
                        break;
                    }
                    if(searchStr.equalsIgnoreCase(srcStr.substring(i,i+searchStrLength))){
                        count++;
                    }
                }
                long   endTime = System.currentTimeMillis();
                System.out.println("顺序遍历的方式耗时:"+(endTime-starTime));
            }
        }
    }
    /*
     *间隔遍历
     * @param srcStr
     * @param searchStr
     */
    public static void strCountWithInteratorTwo(String srcStr,String searchStr){
        if(srcStr.length() > 0 && searchStr.length() > 0) {
            int srcStrLength = srcStr.length();
            int searchStrLength = searchStr.length();
            if(srcStrLength > searchStrLength){
                int j = 0 ;
                int count = 0 ;
                long startTime = System.currentTimeMillis();
                while((j+searchStrLength)<=srcStrLength){
                    if(searchStr.equalsIgnoreCase(srcStr.substring(j,j+searchStrLength))){
                        count++;
                    }
                    j=j+searchStrLength-1;
                }
                System.out.println("间隔遍历方式统计出现的次数为："+count);
                System.out.println("间隔遍历的方式耗时："+(System.currentTimeMillis()-startTime));
            }
        }

    }
    public static void main(String[] args){
        strCountWhitIterator("aaaaabbaaaaaaaaaaaaaaaaasdddgfdgxgdhdhgdhdgdfsfsfsfafdsfdsfsfgdgdgdxdfdgfdaaaaaaaaaa","aa");
        strCountWithInteratorTwo("aaaaabbaaaaaaaaaaaaaaaaasdddgfdgxgdhdhgdhdgdfsfsfsfafdsfdsfsfgdgdgdxdfdgfdaaaaaaaaaa","aa");


    }
}

