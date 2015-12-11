import java.io.*; 
import java.util.*;; 
import org.apache.mahout.cf.taste.impl.model.file.*;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel.*;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator; 
import org.apache.mahout.cf.taste.impl.model.*; 
import org.apache.commons.csv.*;
import org.apache.mahout.cf.taste.impl.common.*;
import java.util.Map.Entry; 
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray; 
import org.apache.mahout.cf.taste.common.TasteException; 
import org.apache.mahout.cf.taste.eval.*; 
import org.apache.mahout.cf.taste.impl.eval.*;
import org.apache.mahout.common.RandomUtils; 

/**
*Basic implementation of a collaborative filter recommender. 
**/
class CFRecommender {

    private static DataModel dataModel; 
    private MemoryIDMigrator memConverter = new MemoryIDMigrator(); 
    private UserSimilarity similarity; 
    private UserNeighborhood neighborhood = null; 
    private  CachingRecommender recommender = null;  
    private int neighborRange = 15; 
    private FastByIDMap<PreferenceArray> fastPrefMap; 
    private Map<Long,List<Preference>> allPrefs; 

    public static void main(String[] args) throws IOException, TasteException, UnsupportedEncodingException{

	CFRecommender cfr = new CFRecommender(); 
	PrintWriter writ = new PrintWriter("output.txt", "UTF-8"); 
	writ.println("is this like not working"); 
	dataModel = cfr.createModel();
	System.out.println("model has been created"); 
	//build the caching recommender from the genericuser model. 
       	cfr.recommender = cfr.initializeRecommender(dataModel);
        cfr.makeRecommendingSet(cfr.recommender); 
	//cfr.calculateRSME(dataModel); 
    }

    /**
     * Method for calculating the RMSE. 
     **/
    private void calculateRSME(DataModel model) throws TasteException, FileNotFoundException, UnsupportedEncodingException{
	
	RandomUtils.useTestSeed(); 
       	RecommenderBuilder builder = new RecommenderBuilder(){
       		public Recommender buildRecommender(DataModel model) throws TasteException{
	      	    CachingRecommender recommender = initializeRecommender(model); 
		    return recommender; 
		}
        };
	RecommenderEvaluator rmseEval = new RMSRecommenderEvaluator(); 
	double rmse = rmseEval.evaluate(builder, null, model, 0.7, 1.0); 
	System.out.print("rmse is: "); 
	System.out.println(rmse); 
	PrintWriter writer = new PrintWriter("output.txt", "UTF-8"); 
	writer.println(rmse);
	writer.close(); 
	
    }
    

    /**
     *Get a list of recommendations for a specific user. 
     **/
    private List<RecommendedItem>  makeRecommendations(CachingRecommender recommender, long user, int n ) throws TasteException 
    {
	List<RecommendedItem> recommendations = recommender.recommend(user, n);
	return recommendations; 
    }
    
    /**
     *Make a set of users next song recommendation so we can have a start point for content based 
     *recommendation. 
     **/
    private void  makeRecommendingSet(CachingRecommender r) throws TasteException, UnsupportedEncodingException, FileNotFoundException{
	File file = new File("NextSongs.csv"); 
	PrintWriter writer = new PrintWriter(file); 
	
	for(Entry<Long, List<Preference>> entry : allPrefs.entrySet()){
	    System.out.println("Loop"); 
	    long id = entry.getKey();
	    List<RecommendedItem> nextList = r.recommend(id,1);
	    if(nextList.size() > 0){
		RecommendedItem nextSong = nextList.get(0); 
		writer.println(memConverter.toStringID(id) + "," + nextSong.getItemID()); 
	    }else
		writer.println(memConverter.toStringID(id) + ",null" ); 
	}
	writer.close(); 
    }
    
    /**
     *Method to calculate the similarity of the model and the neighborhood to be used. 
     **/
    private CachingRecommender initializeRecommender(DataModel model) throws TasteException{
	similarity = new PearsonCorrelationSimilarity(model); 
	//similarity = new SpearmanCorrelationSimilarity(model); 
	neighborhood = new NearestNUserNeighborhood(neighborRange, similarity, model);
	GenericUserBasedRecommender gr = new GenericUserBasedRecommender(model, neighborhood, similarity);
	return new CachingRecommender(gr); 
    }

    /**
     *Creates the dataModel for the recommendation system. 
     *
     **/
    private GenericDataModel createModel() throws FileNotFoundException, IOException{
	File train_triplets = new File("training.csv"); //add your path.
	CSVParser parser = new CSVParser(new InputStreamReader(new FileInputStream(train_triplets))); 
        allPrefs = new HashMap<Long, List<Preference>>(); 
	
	String[] line; 
	while((line = parser.getLine()) != null){
	    String userID = line[0]; 
	    String songID = line[1]; 
	    int playCount = Integer.parseInt(line[2]); 
	    
	    //    System.out.println(userID); 
	    long userLong = memConverter.toLongID(userID);
	    memConverter.storeMapping(userLong, userID); 
       	    long songLong = memConverter.toLongID(songID);
	    memConverter.storeMapping(songLong, songID); 
	    
	    List<Preference> userPrefs; 
      	    if((userPrefs = allPrefs.get(userLong)) == null){
	    	userPrefs = new ArrayList<Preference>();  
		allPrefs.put(userLong, userPrefs); 
	    }
	    userPrefs.add(new GenericPreference(userLong, songLong, playCount)); 
	}

	fastPrefMap = new FastByIDMap<PreferenceArray>(); 
	for(Entry<Long, List<Preference>> entry : allPrefs.entrySet()){
            convertToPercentages(entry.getValue());
            fastPrefMap.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue())); 
	}
	
	return new GenericDataModel(fastPrefMap);   
    }


    /** 
     * Reduces the preferences to values that fall in the range of [0,1] by
     * dividing the value by the sum of all values in the array.
     */
    private void convertToPercentages(List<Preference> prefs) {
        float sum = 0;
        for (Preference p : prefs) {
            sum += p.getValue();
        }
        for (Preference p : prefs) {
            p.setValue(p.getValue() / sum);
        }
    }
}
