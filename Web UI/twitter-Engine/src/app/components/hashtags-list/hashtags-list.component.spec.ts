import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HashtagsListComponent } from './hashtags-list.component';

describe('HashtagsListComponent', () => {
  let component: HashtagsListComponent;
  let fixture: ComponentFixture<HashtagsListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HashtagsListComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HashtagsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
