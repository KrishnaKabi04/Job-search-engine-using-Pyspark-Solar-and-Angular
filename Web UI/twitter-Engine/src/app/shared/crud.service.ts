import { Injectable } from '@angular/core';

import { retry, catchError } from 'rxjs/operators';
import { Observable, throwError } from 'rxjs';
import { HttpClient, HttpHeaders, HttpErrorResponse, HttpResponse } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})

export class CrudService {

  // REST API
  endpoint = 'http://localhost:8080/api/search/';
  public limit: number = 5;

  constructor(private httpClient: HttpClient) { }

  private handleError(error: HttpErrorResponse) {
    return throwError(error.message);
  }

  httpHeader = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json'
    })
  }  


  getDataApi(url: string) {
    return this.httpClient.get(this.endpoint +url)
      .pipe(
        catchError(this.handleError)
      );
  }


}