export interface IProperty {
  url?: string;
  loading?: boolean;                      // loader
  itemsPerPage?: number;                  // total items per page
  total?: number;                         // total records count
  p?: number;                             // page
  sizeLimit?: number;                     // file size limit --- 5000000 means 5MB
  title?: string;
  text?: string;
  successTitle?: string;
  successText?: string;
  len?: any;
  items?: any[];
  sub?: any;
  successMsgType?: number;
  showMsg?: boolean;
  successMsg?: string;
  msg?: string;
  categoryId?: string;
  subCategoryId?: string;
  merchantId?: string;
  userId?: string;
  status?: string;
  userPlaceholder?: string;
  transactionId?: string;
  subCategoryName?: string;
  categoryName?: string;
  credits?: number;
  commissionFee?: number;
  amount?: number;
  fullName?: string;
  email?: string;
  countryCode?: string;
  phoneNumber?: string;
  showList?: boolean;
  showBtnText?: string;
  value?: Date;
  data?: any;
  hashtags?: any;
  msgs?: any;
  errorMsg?: string;
  labels?: any[];
  productPlaceholder?: string;
  userobj?: string;
}
