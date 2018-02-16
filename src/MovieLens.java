
/** Write a Java application to execute MapReduce to find each of the followings:
> - Task 1. MapReduce to find the top 25 rated movies in the [movieLens](http://files.grouplens.org/datasets/movielens/ml-1m.zip) dataset
> - Task 2. MapReduce to find the number of males and females in the movielens dataset
> - Task 3. MapReduce to find the number of movies rated by different users

*/

public class MovieLens {
    
    public static void getTopRatedMovies() {
        String map = "function () { emit(this.MovieID, 1); }";
        String reduce = "function (key, values) { return Array.sum(values); }";
        
        String inputCollection = "ratings";
        String outputCollection = "topMovies";
        
        MovieLensUtil.mapReduce(inputCollection, map, reduce, outputCollection);
//        then use mongo-shell to find the top 25 since it is the best way to get enriched information including movie titles
    }
    
    public static void countByGender() {
        String map = "function () { emit(this.Gender, 1); }";
        String reduce = "function (key, values) { return Array.sum(values); }";
        
        String inputCollection = "users";
        String outputCollection = "countByGender";
        
        MovieLensUtil.mapReduce(inputCollection, map, reduce, outputCollection);
    }
    
    public static void countRatingsByUser() {
        String map = "function () { emit(this.UserID, 1); }";
        String reduce = "function (key, values) { return Array.sum(values); }";
        
        String inputCollection = "ratings";
        String outputCollection = "ratingsByUser";
        
        MovieLensUtil.mapReduce(inputCollection, map, reduce, outputCollection);
    }
    
    public static void main(String[] args) {
        MovieLensUtil.loadDataToMongo();
        getTopRatedMovies();
        countByGender();
        countRatingsByUser();
    }
}