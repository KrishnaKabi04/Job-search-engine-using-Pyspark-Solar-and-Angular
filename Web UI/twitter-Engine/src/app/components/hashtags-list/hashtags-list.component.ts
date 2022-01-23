import { Component, OnInit,ViewChild,ElementRef } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { CrudService } from "../../shared/crud.service";
import { IProperty } from '../../shared/property';

@Component({
  selector: 'app-hashtags-list',
  templateUrl: './hashtags-list.component.html',
  styleUrls: ['./hashtags-list.component.css','../../../assets/css/style.css']
})
export class HashtagsListComponent implements OnInit {

  public parameter: IProperty = {};
  hash: any; 
  sub: any;

  constructor(private route: ActivatedRoute, public crudService: CrudService) { 
    this.parameter.p= 1;
    this.parameter.itemsPerPage = this.crudService.limit;
    this.sub = this.route.params.subscribe(params => {
      this.hash = params['id'];
   });
    this.getHashtag(this.parameter.p);
  }

  ngOnInit() {
}


getPage(page: number){
  this.parameter.p=page;
  this.getHashtag(page);

}

getHashtag(page: number){
  this.parameter.loading = true;
  var skip = (page-1) * this.crudService.limit;
  this.parameter.url = 'hashtag?skip=' + skip + '&limit=' + this.crudService.limit + '&tag=' + this.hash;
    this.crudService.getDataApi(this.parameter.url)
       .subscribe((response :any) => {
          this.parameter.total = response.count;
          this.parameter.data = response.data;
        },
        error => {
          console.log(error);
        });
}



}
