const express = require("express");
const { open } = require("sqlite");
const path = require("path");
const sqlite3 = require("sqlite3");

let database;
const app = express();
app.use(express.json());

const initializeDBandServer = async () => {
  try {
    database = await open({
      filename: path.join(__dirname, "movies.db"),
      driver: sqlite3.Database,
    });
    app.listen(3000, () => {
      console.log("Server is running on http://localhost:3000/");
    });
  } catch (error) {
    console.log(`Database error is ${error.message}`);
    process.exit(1);
  }
};

initializeDBandServer();

//api 1
app.get("/api/v1/longest-duration-movies", async (request, response) => {
  const getQuery = `SELECT tconst,primaryTitle,runtimeMinutes,genres FROM movies ORDER BY runtimeMinutes DESC LIMIT 10;`;
  const responseResult = await database.all(getQuery);
  response.send(responseResult);
});

//post details
app.post("/api/v1/new-movie", async (request, response) => {
  const details = request.body;

  let { tconst, titleType, primaryTitle, runtimeMinutes, genres } = details;

  const query = `
  insert 
  into 
  movies
    (tconst,titleType,primaryTitle,runtimeMinutes,genres) 
  values
    (
        "${tconst}",
         "${titleType}",
         "${primaryTitle}",
         ${runtimeMinutes},
         "${genres}"
        );`;
  const dbResponse = await database.run(query);
  response.send("Success");
});

app.get("/api/v1/top-rated-movies", async (request, response) => {
  const getQuery = `SELECT m.tconst,m.primaryTitle,m.genres,r.averageRating FROM movies m JOIN ratings r ON m.tconst = r.tconst WHERE averageRating > 6 ORDER BY averageRating;`;
  const responseResult = await database.all(getQuery);
  response.send(responseResult);
});

app.get("/api/v1/genre-movies-with-subtotals", async (request, response) => {
  const getQuery = `SELECT
  genres,
  primaryTitle,
  numVotes
    FROM
    (
        SELECT
        genres AS genres,
        primaryTitle,
        numVotes
        FROM
        movies
        JOIN ratings ON movies.tconst = ratings.tconst
        UNION
        ALL
        SELECT
        genres AS genres,
        'TOTAL' AS primaryTitle,
        totalVotes AS numVotes
        FROM
        (
            SELECT
            genres,
            SUM(numVotes) AS totalVotes
            FROM
            movies
            JOIN ratings ON movies.tconst = ratings.tconst
            GROUP BY
            genres
        ) t
    )
    ORDER BY
    genres,
    CASE
        WHEN primaryTitle = 'TOTAL' THEN 1
        ELSE 0
    END,
    primaryTitle;`;
  const responseResult = await database.all(getQuery);
  response.send(responseResult);
});

app.post("/api/v1/update-runtime-minutes", async (request, response) => {
  const todoDetails = request.body;

  let { tconst, titleType, primaryTitle, runtimeMinutes, genres } = todoDetails;

  const playerQuery = `
  UPDATE
  movies
SET
  runtimeMinutes = CASE
    WHEN genres = 'Documentary' THEN runtimeMinutes + 15
    WHEN genres = 'Animation' THEN runtimeMinutes + 30
    ELSE runtimeMinutes + 45
  END;`;
  const dbResponse = await database.get(playerQuery);

  response.send("Success");
});

module.exports = app;
