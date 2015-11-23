import java.io.*; 
import java.util.*; 
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
import org.apache.commons.csv.CSVParser;
import org.apache.mahout.cf.taste.impl.common.*;
import java.util.Map.Entry; 
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray; 
import org.apache.mahout.cf.taste.common.TasteException; 
import org.apache.mahout.cf.taste.eval.*; 
import org.apache.mahout.cf.taste.impl.eval.*;
import org.apache.mahout.common.RandomUtils; 

/**
*Basic implementation of a collaborative filter recommender. There are a couple of issues to be aware of 
* 1. It is not quite finished and is untested. 
* 2. The alpha-numeric string that represents the user sometimes ends up being negative when I convert to a float. I need to look into ways to fix this. 
* 3. She talked about normalizing the data since some users will listen to a song alot and not like it as much as somebody who listens once.  I am not sure how to go about this, but will look into it. 
*    
**/
class CFRecommender {

    private static DataModel dataModel; 
    private MemoryIDMigrator memConverter = new MemoryIDMigrator(); 
    private UserSimilarity similarity; 
    private UserNeighborhood neighborhood = null; 
    //  private Recommender recommender = null; 
    private  CachingRecommender recommender = null;  
    private int neighborRange = 15; 
      
    public static void main(String[] args) throws IOException, TasteException, UnsupportedEncodingException{

	CFRecommender cfr = new CFRecommender(); 
	PrintWriter writ = new PrintWriter("output.txt", "UTF-8"); 
	writ.println("is this like not working"); 
	dataModel = cfr.createModel();
	System.out.println("model has been created"); 
	//build the caching recommender from the genericuser model. 
	//cfr.recommender = cfr.initializeRecommender(dataModel);
	cfr.calculateRSME(dataModel); 
    }

    /**
     * Method for trying to calculate the RMSE. 
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
	writer.println("finished at least "); 
	writer.println(rmse);
	writer.close(); 
	
    }
    

    /**
     *Get a list of recommendations for a specific user. (UNTESTED)  
     **/
    private List<RecommendedItem>  makeRecommendations(GenericUserBasedRecommender recommender, long user, int n ) throws TasteException 
    {
	List<RecommendedItem> recommendations = recommender.recommend(user, n);
	return recommendations; 
    }
    
    //Little debugging method. 
    private void printSimilarities(UserSimilarity pSim) throws TasteException{
		//	System.out.println(pSim.userSimilarity(5582627969861115877,2427962755229849655)); 
	//System.out.println(pSim.userSimilarity(5582627969861115877,9214985604270756226)); 
	//	System.out.println(pSim.userSimilarity(1,2)); 
    }

    /**Make sure that by passing it as a DataModel and not GenericDataModel that it still works, Maybe try to get this working better with different kinds of correlations.  
     *
     **/
    private CachingRecommender initializeRecommender(DataModel model) throws TasteException{
	similarity = new PearsonCorrelationSimilarity(model); 
	
	//Printing a few similarities just to see what they look like. 
	printSimilarities(similarity); 
	
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
	Map<Long,List<Preference>> allPrefs = new HashMap<Long, List<Preference>>(); 
	
	String[] line; 
	while((line = parser.getLine()) != null){
	    String userID = line[0]; 
	    String songID = line[1]; 
	    int playCount = Integer.parseInt(line[2]); 
	    
	    //Convert the two Strings for user and song to usable longs. Im doing this because the fileDataModel doesn't work for our data.  
	    long userLong = memConverter.toLongID(userID); //Coming out negative alot ? 
	    long songLong = memConverter.toLongID(songID); 
	    
	    //Print out the userLong just to see what they are. 
	    //	    System.out.println(userLong); 

	    List<Preference> userPrefs; 
      	    if((userPrefs = allPrefs.get(userLong)) == null){
	    	userPrefs = new ArrayList<Preference>();  
		allPrefs.put(userLong, userPrefs); 
	    }
	    userPrefs.add(new GenericPreference(userLong, songLong, playCount)); 
	}

	FastByIDMap<PreferenceArray> fastPrefMap = new FastByIDMap<PreferenceArray>(); 
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
