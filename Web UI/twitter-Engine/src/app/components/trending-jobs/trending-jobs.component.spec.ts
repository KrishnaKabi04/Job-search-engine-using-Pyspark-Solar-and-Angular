import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TrendingJobsComponent } from './trending-jobs.component';

describe('TrendingJobsComponent', () => {
  let component: TrendingJobsComponent;
  let fixture: ComponentFixture<TrendingJobsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TrendingJobsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TrendingJobsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
