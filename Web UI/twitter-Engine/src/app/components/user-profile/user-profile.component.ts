import { Component, OnInit,ViewChild,ElementRef } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { CrudService } from "../../shared/crud.service";
import { IProperty } from '../../shared/property';

@Component({
  selector: 'app-user-profile',
  templateUrl: './user-profile.component.html',
  styleUrls: ['./user-profile.component.css','../../../assets/css/style.css']
})
export class UserProfileComponent implements OnInit {
  public parameter: IProperty = {};
  userID: any; 
  sub: any;
  userobj: any;
  totl: any;

  constructor(private route: ActivatedRoute, public crudService: CrudService) { 
    this.parameter.p= 1;
    this.parameter.itemsPerPage = this.crudService.limit;
    this.sub = this.route.params.subscribe(params => {
      this.userID = params['id'];
   });
    this.userprofile(this.parameter.p);
  }

  ngOnInit() {
  }

  getPage(page: number){
    this.parameter.p=page;
    this.userprofile(page);
  
  }
  
  userprofile(page: number){
    this.parameter.loading = true;
    var skip = (page-1) * this.crudService.limit;
    this.parameter.url = 'user?skip=' + skip + '&limit=' + this.crudService.limit + '&id=' + this.userID;
      this.crudService.getDataApi(this.parameter.url)
         .subscribe((response :any) => {
            this.totl = response.count;
            this.parameter.total = response.count;
            this.parameter.data = response.tweets;
            this.userobj = response.user;
          },
          error => {
            console.log(error);
          });
  }

}
