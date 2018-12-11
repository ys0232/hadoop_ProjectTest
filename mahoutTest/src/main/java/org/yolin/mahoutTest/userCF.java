package org.yolin.mahoutTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class userCF {
    private final static int neighborhood_num=2;
    private final static int recommender_num=3;
    public static void main(String[] args) throws IOException, TasteException {
        String dir="/home/yolin/gitworkspace/hadoop_ProjectTest/mahoutTest/";
        String file=dir+"/data/item.csv";
        FileDataModel model = new FileDataModel(new File(file));
        UserSimilarity user = new EuclideanDistanceSimilarity(model);
        NearestNUserNeighborhood neighborhood = new NearestNUserNeighborhood(neighborhood_num,user,model);
        Recommender r=new GenericUserBasedRecommender(model,neighborhood,user);
        LongPrimitiveIterator iter=model.getUserIDs();

        while (iter.hasNext()){
            long uid=iter.nextLong();
            List list=r.recommend(uid,recommender_num);
            System.out.printf("uid: %s ",uid);
            for (Object item:list){
                RecommendedItem ritem=(RecommendedItem)item;
                System.out.printf("(%s,%f)",ritem.getItemID(),ritem.getValue());
            }
            System.out.println();
        }


    }
}
