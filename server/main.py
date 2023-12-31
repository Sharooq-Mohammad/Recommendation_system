from fastapi import FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import List

from database import MongoConnection

app = FastAPI(debug=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")
db_connection = MongoConnection()

@app.get("/")
async def home_page(request: Request, error: str=None) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request, name="home.html", context={"error_msg": error}
    )

@app.get("/search") 
async def search_movies(request: Request, query: str, page: int= 0) -> HTMLResponse:
    # print("="*20, "\n", query, "\n", "="*20)
    movies_list, next_page = db_connection.find_movies_by_input(query, page)

    response_ctx = { 
        "documents": movies_list, 
        "next": next_page,
        "query": query,
        "prev": page - 1
    }
    
    return templates.TemplateResponse(
        request=request, name="search.html", context=response_ctx
    )

@app.get("/stage")
async def staged_movies(request: Request, movies: List[str]= Query(title="movies", default_factory=list)) -> HTMLResponse:
    if len(movies) > 3:
        err_msg = "Cannot stage more than 3 movies. Please retry again."
        return RedirectResponse(f"/?error={err_msg}")
    elif len(movies) < 1:
        err_msg = "Add movies to stage. Please retry again."
        return RedirectResponse(f"/?error={err_msg}")
    else:
        print("="*20, "\n", movies, "\n", "="*20)
        response = db_connection.get_staged_movies(movies)
        if len(response) < len(movies):
            titles = [doc["title"] for doc in response]
            missing_movies = [movie_name for movie_name in movies if movie_name not in titles]
            missing_movies = ','.join(missing_movies)

            err_msg = f"Invalid movie title: {missing_movies}"
            return RedirectResponse(f"/?error={err_msg}")
        else:
            response_ctx = { "documents": response }
            return templates.TemplateResponse(
                request=request, name="stage.html", context=response_ctx
                )


@app.get("/generate")
async def generate_recommendations(request: Request, samples: List[str]=Query(title="samples", default_factory=list)) -> HTMLResponse:
    movies = [movie for movie in samples if movie]

    if len(movies) > 3:
        err_msg = "Cannot stage more than 3 movies. Please retry again."
        return RedirectResponse(f"/?error={err_msg}")
    if len(movies) < 1:
        err_msg = "Add movies to stage. Please retry again."
        return RedirectResponse(f"/?error={err_msg}")
    
    response = db_connection.get_recommended_movies(movies)
    response_ctx = { "documents": response }

    return templates.TemplateResponse(
        request=request, name="generate.html", context=response_ctx
    )
