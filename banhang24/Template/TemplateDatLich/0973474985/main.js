var vTemplate = new Vue({
    el: "#idtemplate",
    data: {
        currentPage: 1,
        maxPage: 5,
        ListChiNhanh: [],
        ChiNhanhSelected: 0,
        ListHangHoa: [],
        ListTimeSelect: [],
        TimeSelected: 0,
        DateStart: "datestartvalue",
        DateEnd: "dateendvalue",
        DateInterval: "dateintervalvalue",
        DateBefore: "datebeforevalue",
        Subdomain: "subdomainvalue"
    },
    methods: {
        next: function () {
            let self = this;
            if (self.currentPage === self.maxPage) {
                self.currentPage = 1;
            }
            else {
                self.currentPage += 1;
            }
        },
        prev: function () {
            let self = this;
            if (self.currentPage === 1) {
                self.currentPage = self.maxPage;
            }
            else {
                self.currentPage -= 1;
            }
        },
        InitChiNhanh: function () {
            return [{ Id: '1', TenChiNhanh: 'Chi nhánh 1', DiaChi: 'Địa chỉ 1', SoDienThoai: '1' },
            { Id: '2', TenChiNhanh: 'Chi nhánh 2', DiaChi: 'Địa chỉ 2 12312351 1231231', SoDienThoai: '2' },
            { Id: '3', TenChiNhanh: 'Chi nhánh 2', DiaChi: 'Địa chỉ 2 12312351 1231231', SoDienThoai: '2' },
            { Id: '4', TenChiNhanh: 'Chi nhánh 2', DiaChi: 'Địa chỉ 2 12312351 1231231', SoDienThoai: '2' },
            { Id: '5', TenChiNhanh: 'Chi nhánh 2', DiaChi: 'Địa chỉ 2 12312351 1231231', SoDienThoai: '2' },
            { Id: '6', TenChiNhanh: 'Chi nhánh 2', DiaChi: 'Địa chỉ 2 12312351 1231231', SoDienThoai: '2' },
            { Id: '7', TenChiNhanh: 'Chi nhánh 2', DiaChi: 'Địa chỉ 2 12312351 1231231', SoDienThoai: '2' },
            { Id: '8', TenChiNhanh: 'Chi nhánh 2', DiaChi: 'Địa chỉ 2 12312351 1231231', SoDienThoai: '2' }];
        },
        SelectChiNhanh: function (Id) {
            let self = this;
            self.ChiNhanhSelected = Id;
        },
        InitHangHoa: function () {
            return [{ Id: 1, TenHangHoa: 'Hàng hóa 1', MoTa: 'Đây là hàng hóa 1', DonGia: 10000, Selected: false },
            { Id: 2, TenHangHoa: 'Hàng hóa 2', MoTa: 'Đây là hàng hóa 2', DonGia: 10000, Selected: false },
            { Id: 3, TenHangHoa: 'Hàng hóa 3', MoTa: 'Đây là hàng hóa 3', DonGia: 10000, Selected: false },
            { Id: 4, TenHangHoa: 'Hàng hóa 4', MoTa: 'Đây là hàng hóa 4', DonGia: 10000, Selected: false },
            { Id: 5, TenHangHoa: 'Hàng hóa 5', MoTa: 'Đây là hàng hóa 5', DonGia: 10000, Selected: false },
            { Id: 6, TenHangHoa: 'Hàng hóa 6', MoTa: 'Đây là hàng hóa 6', DonGia: 10000, Selected: false },
            { Id: 7, TenHangHoa: 'Hàng hóa 7', MoTa: 'Đây là hàng hóa 7', DonGia: 10000, Selected: false },
            { Id: 8, TenHangHoa: 'Hàng hóa 8', MoTa: 'Đây là hàng hóa 8', DonGia: 10000, Selected: false },
            { Id: 9, TenHangHoa: 'Hàng hóa 9', MoTa: 'Đây là hàng hóa 9', DonGia: 10000, Selected: false },
            { Id: 10, TenHangHoa: 'Hàng hóa 10', MoTa: 'Đây là hàng hóa 10', DonGia: 10000, Selected: false }];
        },
        SelectHangHoa: function (item) {
            let self = this;
            self.ListHangHoa.filter(p => p.Id === item.Id)[0].Selected = !item.Selected;
        },
        InitTimeSelect: function () {
            let self = this;
            let start = "08:00"
            if(self.DateStart !== "datestartvalue")
            {
                start = self.DateStart;
            }
            let end = "17:30"
            if(self.DateEnd !== "dateendvalue")
            {
                end = self.DateEnd;
            }
            let interval = 30;
            if(self.DateInterval !== "dateintervalvalue"){
                interval = parseInt(self.DateInterval);
            }
            let before = 0;
            if(self.DateBefore !== "datebeforevalue"){
                before = parseInt(self.DateBefore);
            }
            let arrtime = GetTimeInterval(start, end, interval);
            let now = new Date();
            let currentMinute = now.getHours() * 60 + now.getMinutes() + before;
            arrtime.map(function (item) {
                if (item.minute < currentMinute) {
                    item["enable"] = false;
                }
                else {
                    item["enable"] = true;
                }
            });
            return arrtime;
        },
        SelectTime: function (item) {
            let self = this;
            self.TimeSelected = item.minute
        }
    },
    created: function () {
        let self = this;
        self.ListChiNhanh = self.InitChiNhanh();
        self.ListHangHoa = self.InitHangHoa();
        self.ListTimeSelect = self.InitTimeSelect();
    }
});