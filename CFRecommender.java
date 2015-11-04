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
    private Recommender recommender = null; 
    private int neighborRange = 15; 
      
    public static void main(String[] args) throws IOException{

	CFRecommender cfr = new CFRecommender(); 
	dataModel = cfr.createModel();
	cfr.initializeRecommender(dataModel); 
	//make some recommendations. 
    }
    
    //Get a list of recommendations for a specific user. (UNTESTED)  
    private List<RecommendedItem>  makeRecommendations(Recommender recommender, long user, int n ){
	List<RecommendedItem> recommendations = recommender.recommend(user, n);
	return recommendations; 
    }

    //Make sure that by passing it as a DataModel and not GenericDataModel that it still works 
    private void initializeRecommender(DataModel model){
	similarity = new PearsoneCorrelationSimilarity(model); 
	neighborhood = new NearestUserNeightborhood(neighborRange, similarity, model);
	recommender = new GenericUserBasedRecommender(model, neighborhood, similarity); 
    }

    private GenericDataModel createModel() throws FileNotFoundException, IOException{
	File train_triplets = new File("train_triplets/small_test.csv"); //add your path.
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
	    
	    List<Preference> userPrefs; 
      	    if((userPrefs = allPrefs.get(userLong)) == null){
	    	userPrefs = new ArrayList<Preference>();  
		allPrefs.put(userLong, userPrefs); 
	    }
	    userPrefs.add(new GenericPreference(userLong, songLong, playCount)); 
	}

	FastByIDMap<PreferenceArray> fastPrefMap = new FastByIDMap<PreferenceArray>(); 
	for(Entry<Long, List<Preference>> entry : allPrefs.entrySet()){
	    fastPrefMap.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue())); 
	}
	return new GenericDataModel(fastPrefMap);   
    }
}
