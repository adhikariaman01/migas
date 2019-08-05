# Most Popular Movie
- [ ] ```PopularMovies.scala```
- [ ] ```PopularMoviesNicer.scala```

# Movie Similarities
- [ ] Map input ratings to (userID,(movieID,rating))
- [ ] Find every movie pair rated by the same user
    - [ ] This can be done with a ```self-join``` operations
    - [ ] At this point we have (userID,((movieID1,rating1),(movieID2,rating2)))
- [ ] Filter out duplicate pairs
- [ ] Make the movie pairs the key 
    - [ ] map to ((movieID1,rating1),(movieID2,rating2))
- [ ] groupByKey() to get every rating pair found for each movie pair
- [ ] Compute similarity between ratings for each movie in the pair
- [ ] Sort, save, and display results.
- [ ] ``` MovieSimilarities.scala ```

- [ ] ```spark-submit --class MoviesSimilarities.scala PopularMovies.jar```

