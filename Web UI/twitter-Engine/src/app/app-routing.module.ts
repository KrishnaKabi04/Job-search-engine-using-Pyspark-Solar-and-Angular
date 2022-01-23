import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { SearchEngineComponent } from './components/search-engine/search-engine.component';
import { UserProfileComponent } from './components/user-profile/user-profile.component';
import { HashtagsListComponent } from './components/hashtags-list/hashtags-list.component';
import { TrendingJobsComponent } from './components/trending-jobs/trending-jobs.component';


const routes: Routes = [
  { path: '', component: SearchEngineComponent },
  { path: 'trending-jobs', component: TrendingJobsComponent },
  { path: 'profile/:id', component: UserProfileComponent },
  { path: 'hastags-list/:id', component: HashtagsListComponent }
];



@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})

export class AppRoutingModule { }