import { Component, OnInit,ViewChild,ElementRef } from '@angular/core';
import { Router } from '@angular/router';
import { CrudService } from "../../shared/crud.service";
import { IProperty } from '../../shared/property';

@Component({
  selector: 'app-search-engine',
  templateUrl: './search-engine.component.html',
  styleUrls: ['./search-engine.component.css','../../../assets/css/style.css']
})
export class SearchEngineComponent implements OnInit {
  public parameter: IProperty = {};

  constructor(public crudService: CrudService) { 
    this.parameter.p= 1;
    this.parameter.itemsPerPage = this.crudService.limit;
    this.parameter.data = '';
  }

  ngOnInit() {
 
}

  @ViewChild("mysearch") mysearch: any;

  getPage(page: number) {
    this.parameter.p = page;
    var k = this.mysearch.nativeElement.value;
    this.Search(k, page);
  }

  Search(name: any, page: any) {
    this.parameter.loading = true;
    if (page != 'undefined')
      var skip = (page - 1) * this.crudService.limit;
    else
      var skip = 0;
    this.parameter.url = 'query?skip=' + skip + '&limit=' + this.crudService.limit + '&query=' + name;
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


