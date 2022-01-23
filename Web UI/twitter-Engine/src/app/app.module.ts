import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';
import { FormsModule } from '@angular/forms';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { SearchEngineComponent } from './components/search-engine/search-engine.component';
import { TrendingJobsComponent } from './components/trending-jobs/trending-jobs.component';
import { HashtagsListComponent } from './components/hashtags-list/hashtags-list.component';
import { UserProfileComponent } from './components/user-profile/user-profile.component';
import { NgxPaginationModule } from 'ngx-pagination';

@NgModule({
  declarations: [
    AppComponent,
    SearchEngineComponent,
    TrendingJobsComponent,
    HashtagsListComponent,
    UserProfileComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,
    FormsModule,
    NgxPaginationModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
