﻿var ViewModelHD = function () {
    var self = this;

    var BH_HoaDonUri = '/api/DanhMuc/BH_HoaDonAPI/';
    var DMDoiTuongUri = "/api/DanhMuc/DM_DoiTuongAPI/";
    var Quy_HoaDonUri = '/api/DanhMuc/Quy_HoaDonAPI/';
    var CSKHUri = '/api/DanhMuc/ChamSocKhachHangAPI/';
    var id_donvi = $('#hd_IDdDonVi').val();// get from @Html.Hidden
    var userLogin = $('#txtUserLogin').val();
    var LoaiHoaDonMenu = $('#txtLoaiHoaDon').val();
    console.log('loai hoa don menu ', LoaiHoaDonMenu);
    var _id_NhanVien = $('.idnhanvien').text();
    var _IDNguoiDung = $('.idnguoidung').text();
    var DiaryUri = '/api/DanhMuc/SaveDiary/';
    self.TodayBC = ko.observable('Toàn thời gian');
    const arrSubDomain = ["leeauto", "0973474985", "autosonly"];
    self.isLeeAuto = ko.observable($.inArray(VHeader.SubDomain.toLowerCase(), arrSubDomain) > -1);
    self.ShopCookie = ko.observable($('#txtShopCookie').val());
    self.TenChiNhanh = ko.observableArray();
    self.HoaDons = ko.observableArray();
    self.BH_HoaDon_ChiTiet = ko.observableArray();
    self.NhanViens = ko.observableArray();
    self.PhongBans = ko.observableArray();
    self.GiaBans = ko.observableArray();
    self.NgayLapHD_Update = ko.observable();
    self.InforHDprintf = ko.observableArray();
    self.CTHoaDonPrint = ko.observableArray();
    self.CTHoaDonPrintMH = ko.observableArray();
    // lọc hàng hóa
    self.TT_HoanThanh = ko.observable(true);
    self.TT_DaHuy = ko.observable();
    self.TT_TamLuu = ko.observable(true);
    self.TT_GiaoHang = ko.observable(true);
    self.TT_DaDuyet = ko.observable(true);
    self.La_HDBan = ko.observable(LoaiHoaDonMenu === '1');
    self.La_HDSuaChua = ko.observable(LoaiHoaDonMenu === '25');
    self.LoaiHoaDonMenu = ko.observable(parseInt($('#txtLoaiHoaDon').val()));
    self.Quyen_NguoiDung = ko.observableArray();
    // hoa don ban
    self.RoleView_Invoice = ko.observable(false);
    self.RoleRestore_Invoice = ko.observable(false);
    self.RoleInsert_Invoice = ko.observable(false);
    self.RoleInsert_HoaDonBaoHanh = ko.observable(false);
    self.RoleUpdate_Invoice = ko.observable(false);
    self.RoleDelete_Invoice = ko.observable(false);
    self.RoleExport_Invoice = ko.observable(false);
    self.RoleUpdateImg_Invoice = ko.observable(false);
    self.LaAdmin = ko.observable(VHeader.LaAdmin == 'Admin');
    self.RoleXemTongDoanhThu = ko.observable(false);// quyền "Không" (ngược với bình thường)
    // hd dat
    self.RoleView_Order = ko.observable(false);
    self.RoleRestore_Order = ko.observable(false);
    self.RoleInsert_Order = ko.observable(false);
    self.RoleUpdate_Order = ko.observable(false);
    self.RoleDelete_Order = ko.observable(false);
    self.RoleExport_Order = ko.observable(false);
    self.RoleApprove_Order = ko.observable(false);
    //lenh bao hanh
    self.RoleView_LenhBH = ko.observable(false);
    self.RoleRestore_LenhBH = ko.observable(false);
    self.RoleInsert_LenhBH = ko.observable(false);
    self.RoleUpdate_LenhBH = ko.observable(false);
    self.RoleDelete_LenhBH = ko.observable(false);
    self.RoleExport_LenhBH = ko.observable(false);
    self.RoleApprove_LenhBH = ko.observable(false);
    // hd tra
    self.RoleView_Return = ko.observable(false);
    self.RoleInsert_Return = ko.observable(false);
    self.RoleUpdate_Return = ko.observable(false);
    self.RoleDelete_Return = ko.observable(false);
    self.RoleExport_Return = ko.observable(false);

    self.Show_BtnUpdate = ko.observable(false);
    self.Show_BtnCopy = ko.observable(false);
    self.Show_BtnEdit = ko.observable(false);
    self.Show_BtnDelete = ko.observable(false);
    self.Show_BtnExcelDetail = ko.observable(false);
    self.Show_BtnThanhToanCongNo = ko.observable(false);
    self.Show_BtnOpenHD = ko.observable(false);
    self.Show_BtnXulyDH = ko.observable(false);
    self.Role_PrintHoaDon = ko.observable(false);
    self.Role_HoaHongDichVu_Edit = ko.observable(false);
    self.Role_HoaHongHoaDon_Edit = ko.observable(false);
    self.Role_SuaChiPhiDV = ko.observable(false);
    self.Role_NhapHangTuHoaDon = ko.observable(false);
    self.Role_XuatKho = ko.observable(false);
    self.Show_BtnInsertSoQuy = ko.observable(false);
    self.Show_BtnUpdateSoQuy = ko.observable(false);
    self.Show_BtnDeleteSoQuy = ko.observable(false);
    self.Allow_ChangeTimeSoQuy = ko.observable(false);

    self.BaoHiem = ko.observable(3);
    self.BaoHiemCo = ko.observable(true);
    self.BaoHiemKhong = ko.observable(true);
    self.filter = ko.observable();
    self.filterMaHDGoc = ko.observable();
    self.filterFind = ko.observable();
    self.selectedNV = ko.observable(_id_NhanVien);// NVien lap phieuthu
    self.ID_NhanVieUpdateHD = ko.observable();
    self.error = ko.observable();
    self.booleanAdd = ko.observable(true);
    self.ID_DonViQuiDoi = ko.observable();
    self.filterBangGia = ko.observable();
    self.filterPhongBan = ko.observable();
    self.PhongBanChosed = ko.observableArray();
    self.GiaBanChosed = ko.observableArray();
    self.filterNgayLapHD = ko.observable("0");
    self.filterNgayLapHD_Input = ko.observable(); // ngày cụ thể
    self.filterNgayLapHD_Quy = ko.observable(6); // Theo tháng
    self.BH_HoaDonChiTiets = ko.observableArray(); // split HoaDon = Hoa Don + Chi Tiet
    self.HoaDonDoiTra = ko.observableArray();
    self.LichSuThanhToan = ko.observableArray();
    self.LichSuTraHang = ko.observableArray();
    self.LichSuThanhToanDH = ko.observableArray();
    self.TongSLuong = ko.observable();
    self.MaHoaDonParent = ko.observable();
    self.CongTy = ko.observableArray(); // get infor congty
    self.ThietLap = ko.observableArray(); // ThietLapTinhNang HeThong
    self.ChotSo_ChiNhanh = ko.observableArray();
    self.ListIDNhanVienQuyen = ko.observableArray();
    self.ThietLap_TichDiem = ko.observableArray();
    self.ListCheckBox = ko.observableArray();
    self.NumberColum_Div2 = ko.observable();
    self.NumberColum_Div3 = ko.observable();
    self.columsort = ko.observable('NgayLapHoaDon');
    self.sort = ko.observable(0);
    self.PThucChosed = ko.observableArray();
    self.PTThanhToan = ko.observableArray([
        { ID: '1', TenPhuongThuc: 'Tiền mặt' },
        { ID: '2', TenPhuongThuc: 'POS' },
        { ID: '3', TenPhuongThuc: 'Chuyển khoản' },
        { ID: '4', TenPhuongThuc: 'Thẻ giá trị' }
    ]);
    // sum at footer
    self.TongTienBHDuyet = ko.observable(0);
    self.KhauTruTheoVu = ko.observable(0);
    self.GiamTruBoiThuong = ko.observable(0);
    self.BHThanhToanTruocThue = ko.observable(0);
    self.TongTienThueBaoHiem = ko.observable(0);
    self.TongGiamTruBaoHiem = ko.observable(0);

    self.TongTienHang = ko.observable();
    self.TongThanhToan = ko.observable();
    self.PhaiThanhToanBaoHiem = ko.observable();
    self.BaoHiemDaTra = ko.observable();
    self.TienDatCoc = ko.observable();
    self.TongChiPhi = ko.observable();
    self.TongKhachTra = ko.observable();
    self.KhachCanTra = ko.observable();
    self.TongGiamGia = ko.observable();
    self.TongGiamGiaKM = ko.observable();
    self.TongKhachNo = ko.observable();
    self.TongPhaiTraKhach = ko.observable(0);
    self.TongTienThue = ko.observable(0);
    self.TongNoKhach = ko.observable(0);
    self.TongTienDoiDiem = ko.observable(0);
    self.TongTienTheGTri = ko.observable(0);
    self.TongTienMat = ko.observable(0);
    self.ThanhTienChuaCK = ko.observable(0);
    self.GiamGiaCT = ko.observable(0);
    self.TongChuyenKhoan = ko.observable(0);
    self.TongPOS = ko.observable(0);
    self.TongGiaTriSDDV = ko.observable(0);
    self.ThuTuKhach = ko.observable();
    self.SelectPT = ko.observable();
    self.NoSau = ko.observable();
    self.ThoiGian_ThanhToan = ko.observable(moment(new Date()).format('DD/MM/YYYY HH:mm'));
    self.ListHDisDebit = ko.observableArray();
    self.GhiChu_PhieuThu = ko.observable();
    self.NoHienTai = ko.observable();
    self.TongTT_PhieuThu = ko.observable(0);
    self.ItemHoaDon = ko.observableArray();
    self.TienThua_PT = ko.observable(0);
    //lọc theo đơn vị
    self.ChiNhanhs = ko.observableArray();
    self.MangNhomDV = ko.observableArray();
    self.MangIDDV = ko.observableArray();
    self.filterHangHoa_ChiTietHD = ko.observable();
    // phan trang CTHD
    self.PageSize_CTHD = ko.observable(10);
    self.currentPage_CTHD = ko.observable(0);
    self.fromitem_CTHD = ko.observable(1);
    self.toitem_CTHD = ko.observable();
    self.PageCount_CTHD = ko.observable();
    self.TotalRecord_CTHD = ko.observable(0);
    // the gia tri + chiet khau nv
    self.TienTheGiaTri_PhieuThu = ko.observable();
    self.SoDuTheGiaTri = ko.observable();
    self.TongNapThe = ko.observable();
    self.SuDungThe = ko.observable(0);
    self.HoanTraTheGiaTri = ko.observable(0);

    self.IsGara = ko.observable(false);
    self.NganhKinhDoanh = ko.observable(1);//1.banle, 2.gara, 3.nhahang
    self.TongSoLuongHang = ko.observable(0);
    self.TongTienHangChuaCK = ko.observable(0);
    self.TongGiamGiaHang = ko.observable(0);
    self.TongTienPhuTung = ko.observable(0);
    self.TongTienDichVu = ko.observable(0);
    self.TongTienPhuTung_TruocCK = ko.observable(0);
    self.TongTienDichVu_TruocCK = ko.observable(0);
    self.TongTienPhuTung_TruocVAT = ko.observable(0);
    self.TongTienDichVu_TruocVAT = ko.observable(0);

    self.TongThue_PhuTung = ko.observable(0);
    self.TongCK_PhuTung = ko.observable(0);
    self.TongThue_DichVu = ko.observable(0);
    self.TongCK_DichVu = ko.observable(0);
    self.TongSL_DichVu = ko.observable(0);
    self.TongSL_PhuTung = ko.observable(0);

    self.ChiTietDoiTuong = ko.observableArray();
    self.InforBaoHiem = ko.observableArray();

    var urlBanHang = '';
    switch (self.ShopCookie()) {
        case 'C16EDDA0-F6D0-43E1-A469-844FAB143014':
            urlBanHang = '/g/Gara';
            self.IsGara(true);
            self.NganhKinhDoanh(2);
            break;
        case 'C1D14B5A-6E81-4893-9F73-E11C63C8E6BC':
            urlBanHang = '/$/NhaHang';
            self.NganhKinhDoanh(3);
            break;
        default:
            urlBanHang = '/$/BanLe';
            break;
    }

    var sLoai = '';
    var Key_Form = "KeyInvoices";
    switch (loaiHoaDon) {
        case 0:
        case 1:
            switch (self.LoaiHoaDonMenu()) {
                case 1:
                    sLoai = 'hóa đơn bán hàng';
                    break;
                case 2:
                    sLoai = 'hóa đơn bảo hành';
                    Key_Form = "KeyHDBaoHanh";
                    break;
                case 25:
                    sLoai = 'hóa đơn sửa chữa';
                    Key_Form = "KeyHDSuaChua";
                    break;
            }
            break;
        case 3:
            if (self.LoaiHoaDonMenu() === 0) {
                Key_Form = "KeyOrders";
                sLoai = 'hóa đơn đặt hàng';
            }
            else {
                Key_Form = "KeyBGSuaChua";
                sLoai = 'báo giá sửa chữa';
            }
            break;
        case 32:
            Key_Form = "KeyBGSuaChua";
            sLoai = 'lệnh bảo hành';
            break;
        case 6:
            sLoai = 'hóa đơn trả hàng';
            Key_Form = "KeyReturns";
            break;
        case 25:
            Key_Form = "KeyHDSuaChua";
            sLoai = 'hóa đơn sửa chữa';
            break;
    }
    function PageLoad() {
        console.log('bdt')
        loadCheckbox();
        GetListIDNhanVien_byUserLogin();
        getAllChiNhanh();
        GetCauHinhHeThong();
        getListNhanVien();
        getAllPhongBan();
        getAllGiaBan();
        GetInforCongTy();
        loadMauIn();
        GetDM_NhomDoiTuong_ChiTiets();
        GetAllMauIn_byChiNhanh();
        GetKM_CTKhuyenMai();
        GetAllQuy_KhoanThuChi();
        GetDM_TaiKhoanNganHang();
        GetHT_TichDiem();
        GetAllNhomHangHoas();
    }
    PageLoad();

    function SetDefault_HideColumn() {
        var arrHideColumn = [];
        switch (loaiHoaDon) {
            case 1:
            case 25:
                arrHideColumn = ['madathang', 'email', 'diachi', 'sodienthoai', 'masothue', 'masothueBH', 'khuvuc', 'phuongxa', 'tenchinhanh', 'nguoiban', 'nguoitao',
                    'thanhtienchuack', 'giamgiact', 'giatrisudung', 'tienthue', 'pos', 'chuyenkhoan', 'tiendoidiem', 'thegiatri', 'trangthai'];
                break;
            case 3:
            case 32:
                arrHideColumn = ['email', 'diachi', 'sodienthoai', 'khuvuc', 'phuongxa', 'tenchinhanh', 'nguoiban', 'nguoitao', 'tonggiamgia', 'trangthai'];
                break;
            case 6:
                arrHideColumn = ['mahoadon', 'diachi', 'sodienthoai', 'tenchinhanh', 'phitrahang', 'nguoiban', 'nguoitao', 'tongsaugiamgia', 'trangthai'];
                break;
        }

        var cacheHideColumn = localStorage.getItem(Key_Form);
        if (cacheHideColumn === null || cacheHideColumn === '[]') {
            // hide default some column
            for (let i = 0; i < arrHideColumn.length; i++) {
                LocalCaches.AddColumnHidenGrid(Key_Form, arrHideColumn[i], arrHideColumn[i]);
            }
        }
    }
    function loadCheckbox() {
        if (loaiHoaDon === 1) {
            loaiHoaDon = self.LoaiHoaDonMenu();
        }
        $.getJSON("api/DanhMuc/BaseApi/GetListColumnInvoices?loaiHD=" + loaiHoaDon, function (data) {
            if (loaiHoaDon === 25) {
                self.NumberColum_Div2(Math.ceil(data.length / 3));
            }
            else {
                if (loaiHoaDon === 3 || loaiHoaDon == 32) {
                    data = $.grep(data, function (x) {
                        return $.inArray(x.Key, ['tongchiphi']) === -1;
                    });
                    if (self.LoaiHoaDonMenu() === 0) {
                        data = $.grep(data, function (x) {
                            return $.inArray(x.Key, ['maphieutiepnhan', 'bienso']) === -1;
                        })
                    }
                }
                self.NumberColum_Div2(Math.ceil(data.length / 2));
            }
            self.NumberColum_Div3(Math.ceil(data.length / 3 * 2));
            self.ListCheckBox(data);
        });
    }
    function HideShowColumn() {
        SetDefault_HideColumn();
        LocalCaches.LoadFirstColumnGrid(Key_Form, $('#myList ul li input[type = checkbox]'), self.ListCheckBox());
    }
    $('#myList').on('change', 'ul li input[type = checkbox]', function () {
        var valueCheck = $(this).val();
        LocalCaches.AddColumnHidenGrid(Key_Form, valueCheck, valueCheck);
        $('.' + valueCheck).toggle();
    });
    $('#myList').on('click', 'ul li', function (i) {
        if ($(this).find('input[type = checkbox]').is(':checked')) {
            $(this).find('input[type = checkbox]').prop("checked", false);
        }
        else {
            $(this).find('input[type = checkbox]').prop("checked", true);
        }
        var valueCheck = $(this).find('input[type = checkbox]').val();
        LocalCaches.AddColumnHidenGrid(Key_Form, valueCheck, valueCheck);
        $('.' + valueCheck).toggle();
    });
    function loadQuyenIndex() {
        ajaxHelper('/api/DanhMuc/HT_NguoiDungAPI/' + "GetHT_NhomNguoiDung?idnguoidung=" + _IDNguoiDung + '&iddonvi=' + id_donvi, 'GET').done(function (data) {
            if (data.ID !== null) {
                self.Quyen_NguoiDung(data.HT_Quyen_NhomDTO);
                CheckRole_Invoice();

                // check role update/delete soquy
                self.Show_BtnInsertSoQuy(CheckQuyenExist('SoQuy_ThemMoi'));
                self.Show_BtnUpdateSoQuy(CheckQuyenExist('SoQuy_CapNhat'));
                self.Show_BtnDeleteSoQuy(CheckQuyenExist('SoQuy_Xoa'));
                self.Allow_ChangeTimeSoQuy(CheckQuyenExist('SoQuy_ThayDoiThoiGian'));
                self.Role_HoaHongDichVu_Edit(CheckQuyenExist('BanHang_HoaDongDichVu_CapNhat'))
                self.Role_HoaHongHoaDon_Edit(CheckQuyenExist('BanHang_HoaDongHoaDon_CapNhat'));
                self.Show_BtnThanhToanCongNo(CheckQuyenExist('KhachHang_ThanhToanNo'));

                self.Role_SuaChiPhiDV(CheckQuyenExist('HoaDon_SuaChiPhiDichVu'))
                self.Role_NhapHangTuHoaDon(CheckQuyenExist('NhapHang_ThemMoi'))
                self.Role_XuatKho(CheckQuyenExist('XuatHuy_ThemMoi'));
                self.RoleUpdateImg_Invoice(CheckQuyenExist('HoaDon_CapNhatAnh'));
                self.RoleXemTongDoanhThu(CheckQuyenExist('KhachHang_KhongXemTongCong'));
            }
            else {
                ShowMessage_Danger('Không có quyền xem danh sách ' + sLoai);
            }
        });
    }

    function CheckRole_Invoice() {
        switch (loaiHoaDon) {
            case 1:
            case 25:
                self.RoleRestore_Invoice(CheckQuyenExist('HoaDon_Restore'));
                self.RoleView_Invoice(CheckQuyenExist('HoaDon_XemDS'));
                self.RoleInsert_Invoice(CheckQuyenExist('HoaDon_ThemMoi'));
                self.RoleUpdate_Invoice(CheckQuyenExist('HoaDon_CapNhat'));
                self.RoleDelete_Invoice(CheckQuyenExist('HoaDon_Xoa'));
                self.RoleExport_Invoice(CheckQuyenExist('HoaDon_XuatFile'));
                self.Show_BtnEdit(CheckQuyenExist('HoaDon_SuaDoi'));
                self.Show_BtnOpenHD(CheckQuyenExist('HoaDon_CapNhatHDTamLuu'));
                self.RoleInsert_HoaDonBaoHanh(CheckQuyenExist('HoaDonBaoHanh_ThemMoi'));

                self.Show_BtnExcelDetail(self.RoleExport_Invoice());
                self.Show_BtnCopy(CheckQuyenExist('HoaDon_SaoChep'));
                self.Role_PrintHoaDon(CheckQuyenExist('HoaDon_In'));
                self.ThayDoi_NgayLapHD(CheckQuyenExist('HoaDon_ThayDoiThoiGian'));
                self.ThayDoi_NVienBan(CheckQuyenExist('HoaDon_ThayDoiNhanVien'));

                if (self.RoleView_Invoice()) {
                    SearchHoaDon();
                }
                else {
                    ShowMessage_Danger('Không có quyền xem danh sách ' + sLoai)
                }
                break;
            case 2:
                self.RoleView_Invoice(CheckQuyenExist('HoaDonBaoHanh_XemDS'));
                self.RoleInsert_HoaDonBaoHanh(CheckQuyenExist('HoaDonBaoHanh_ThemMoi'));
                self.RoleUpdate_Invoice(CheckQuyenExist('HoaDonBaoHanh_CapNhat'));
                self.RoleDelete_Invoice(CheckQuyenExist('HoaDonBaoHanh_Xoa'));

                self.Show_BtnCopy(CheckQuyenExist('HoaDonBaoHanh_SaoChep'));
                self.RoleExport_Invoice(CheckQuyenExist('HoaDonBaoHanh_XuatFile'));
                self.Role_PrintHoaDon(CheckQuyenExist('HoaDonBaoHanh_In'));

                if (self.RoleView_Invoice()) {
                    SearchHoaDon();
                }
                else {
                    ShowMessage_Danger('Không có quyền xem danh sách ' + sLoai)
                }
                break;
            case 3:
                self.RoleView_Order(CheckQuyenExist('DatHang_XemDS'));
                self.RoleRestore_Order(CheckQuyenExist('DatHang_Restore'));
                self.RoleInsert_Order(CheckQuyenExist('DatHang_ThemMoi'));
                self.RoleUpdate_Order(CheckQuyenExist('DatHang_CapNhat'));
                self.RoleDelete_Order(CheckQuyenExist('DatHang_Xoa'));
                self.RoleExport_Order(CheckQuyenExist('DatHang_XuatFile'));
                self.RoleApprove_Order(CheckQuyenExist('DatHang_DuyetBaoGia'));

                self.Show_BtnCopy(CheckQuyenExist('DatHang_SaoChep'));
                self.Role_PrintHoaDon(CheckQuyenExist('DatHang_In'));
                self.Show_BtnExcelDetail(self.RoleExport_Order());
                self.ThayDoi_NgayLapHD(CheckQuyenExist('DatHang_ThayDoiThoiGian'));
                self.ThayDoi_NVienBan(CheckQuyenExist('DatHang_ThayDoiNhanVien'));

                if (self.RoleView_Order()) {
                    SearchHoaDon();
                }
                else {
                    ShowMessage_Danger('Không có quyền xem danh sách ' + sLoai)
                }
                break;
            case 32:
                self.RoleView_LenhBH(CheckQuyenExist('LenhBaoHanh_XemDS'));
                self.RoleRestore_LenhBH(CheckQuyenExist('LenhBaoHanh_Restore'));
                self.RoleInsert_LenhBH(CheckQuyenExist('LenhBaoHanh_ThemMoi'));
                self.RoleUpdate_LenhBH(CheckQuyenExist('LenhBaoHanh_CapNhat'));
                self.RoleDelete_LenhBH(CheckQuyenExist('LenhBaoHanh_Xoa'));
                self.RoleExport_LenhBH(CheckQuyenExist('LenhBaoHanh_XuatFile'));
                self.RoleApprove_LenhBH(CheckQuyenExist('LenhBaoHanh_DuyetLenh'));

                self.Show_BtnCopy(CheckQuyenExist('LenhBaoHanh_SaoChep'));
                self.Role_PrintHoaDon(CheckQuyenExist('LenhBaoHanh_In'));
                self.Show_BtnExcelDetail(self.RoleExport_LenhBH());
                self.ThayDoi_NgayLapHD(CheckQuyenExist('LenhBaoHanh_ThayDoiThoiGian'));

                if (self.RoleView_LenhBH()) {
                    SearchHoaDon();
                }
                else {
                    ShowMessage_Danger('Không có quyền xem danh sách ' + sLoai)
                }
                break;
            case 6:
                self.RoleView_Return(CheckQuyenExist('TraHang_XemDS'));
                self.RoleInsert_Return(CheckQuyenExist('TraHang_ThemMoi'));
                self.RoleUpdate_Return(CheckQuyenExist('TraHang_CapNhat'));
                self.RoleDelete_Return(CheckQuyenExist('TraHang_Xoa'));
                self.RoleExport_Return(CheckQuyenExist('TraHang_XuatFile'));

                self.Show_BtnCopy(CheckQuyenExist('TraHang_SaoChep'));
                self.Role_PrintHoaDon(CheckQuyenExist('TraHang_In'));
                self.Show_BtnExcelDetail(self.RoleExport_Return());

                CheckQuyen_HoaDonMua();

                if (self.RoleView_Return()) {
                    SearchHoaDon();
                }
                else {
                    ShowMessage_Danger('Không có quyền xem danh sách ' + sLoai)
                }
                break;
        }

        if (self.NganhKinhDoanh() === 3) {
            self.Show_BtnCopy(false);
        }
    }

    function GetListIDNhanVien_byUserLogin() {
        ajaxHelper(CSKHUri + 'GetListNhanVienLienQuanByIDLoGin_inDepartment?idnvlogin=' + _id_NhanVien
            + '&idChiNhanh=' + id_donvi + '&funcName=' + funcName, 'GET').done(function (data) {
                self.ListIDNhanVienQuyen(data);
                loadQuyenIndex();
            })
    }
    function UpdateIDDoiTuong_inHoaDon_andPhieuThu() {
        ajaxHelper(BH_HoaDonUri + 'UpdateIDDoiTuong_inHoaDon_andPhieuThu', 'GET').done(function (data) {
        })
    }

    self.gotoGara = function () {
        if (self.IsGara()) {
            var newwindow = window.open(urlBanHang, '_blank');
            var popupTick = setInterval(function () {
                if (newwindow.closed) {
                    clearInterval(popupTick);
                    SearchHoaDon();
                }
            }, 500);
        }
        else {
            window.open(urlBanHang, '_blank');
        }
    }

    self.clickbanhang = function () {
        localStorage.setItem('fromHoaDon', true);
        self.gotoGara();
    }
    self.selectedCN = function (item) {
        $("#iconSort").remove();
        self.columsort('NgayLapHoaDon');
        self.sort(0);
        var arrDV = [];
        for (let i = 0; i < self.MangNhomDV().length; i++) {
            if ($.inArray(self.MangNhomDV()[i], arrDV) === -1) {
                arrDV.push(self.MangNhomDV()[i].ID);
            }
        }
        if ($.inArray(item.ID, arrDV) === -1) {
            self.MangNhomDV.push(item);
        }
        SearchHoaDon();
        //$('#choose_DonVi input').remove();
        // thêm dấu check vào đối tượng được chọn
        $('#selec-all-DV li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('.fa-check').remove();
                $(this).append('<i class="fa fa-check check-after-li" style="display:block"></i>')
            }
        });
        $('#choose_TenDonVi input').remove();
    }
    self.CloseDV = function (item) {
        self.MangNhomDV.remove(item);
        if (self.MangNhomDV().length === 0) {
            $('#choose_TenDonVi').append('<input type="text" id="dllChiNhanh" readonly="readonly" class="dropdown" placeholder="Chọn chi nhánh">');
        }
        SearchHoaDon();
        // remove check
        $('#selec-all-DV li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('.fa-check').remove();
            }
        });
    }
    self.showPopupNCC = function () {
        self.resetNhaCungCap();
        $('#modalPopuplg_NCC').modal('show');
        $('#modalPopuplg_NCC').on('shown.bs.modal', function () {
            $('#txtTenDoiTuong').select();
        })
        $('#lblTitleNCC').html("Thêm nhà cung cấp")
    };
    self.HuyHoaDon_updateChoThanhToan = function (item) {
        var msgBottom = '';
        var msgDialog = '';
        var idHoaDon = item.ID;
        var urlCheck = BH_HoaDonUri + 'GetDSHoaDon_chuaHuy_byIDDatHang/' + idHoaDon;
        switch (item.LoaiHoaDon) {
            case 1:
            case 0:
                msgBottom = "Hóa đơn đã có trả hàng, không thể hủy";
                break;
            case 3:
                msgBottom = "Phiếu đặt hàng đã có hóa đơn, không thể hủy";
                break;
            case 32:
                msgBottom = "Lệnh bảo hành đã có hóa đơn, không thể hủy";
                break;
            case 6:
                msgBottom = "Phiếu trả hàng đã có hóa đơn, không thể hủy";
                break;
            case 25:
                urlCheck = '/api/DanhMuc/GaraAPI/CheckHoaDon_DaXuLy?idHoaDon=' + idHoaDon + '&loaiHoaDon=8';
                msgBottom = "Hóa đơn đã có phiếu xuất kho, không thể hủy";
                break;
        }
        // huy hoadon : neu dang co tra hang --> khong duoc huy 
        // huy dat hang: neu dang co HD tao tu HD dat hang --> khong duoc huy
        ajaxHelper(urlCheck, 'GET').done(function (x) {
            if (x === true) {
                ShowMessage_Danger(msgBottom);
                return;
            }
            else {
                if (item.LoaiHoaDon !== 3 && item.LoaiHoaDon !== 32) {
                    msgDialog = 'Có muốn hủy hóa đơn <b>' + item.MaHoaDon + '</b> cùng những phiếu liên quan không?'
                }
                else {
                    if (item.KhachDaTra > 0) {
                        msgDialog = 'Có muốn hủy hóa đơn <b>' + item.MaHoaDon + '</b> cùng tiền đặt cọc không?'
                    }
                    else {
                        msgDialog = 'Có muốn hủy hóa đơn <b>' + item.MaHoaDon + '</b> cùng những phiếu liên quan không?'
                    }
                }
                // move dialogConfirm() in this
                dialogConfirm('Thông báo xóa', msgDialog, function () {
                    $.ajax({
                        type: "POST",
                        url: BH_HoaDonUri + "Huy_HoaDon?id=" + idHoaDon + '&nguoiSua=' + userLogin + '&iddonvi=' + id_donvi,
                        dataType: 'json',
                        contentType: 'application/json',
                        success: function (result) {
                            ShowMessage_Success("Cập nhật " + sLoai + " thành công");
                            SearchHoaDon();
                            var objDiary = {
                                ID_NhanVien: _id_NhanVien,
                                ID_DonVi: id_donvi,
                                ChucNang: 'Danh mục ' + sLoai,
                                NoiDung: "Xóa " + sLoai + ": " + item.MaHoaDon,
                                NoiDungChiTiet: "Xóa ".concat(sLoai, ": ", item.MaHoaDon, ', Người xóa: ', userLogin),
                                LoaiNhatKy: 3
                            };
                            if (item.LoaiHoaDon !== 3 && item.LoaiHoaDon !== 32) {
                                // HuyHD : tru diem (cong diem am)
                                // Huy TraHang: cong diem
                                // HuyDatHang: khong thuc hien gi ca
                                var diemGiaoDich = item.DiemGiaoDich;
                                if (diemGiaoDich > 0 && item.ID_DoiTuong !== null) {
                                    if (item.LoaiHoaDon === 1 || item.LoaiHoaDon === 25) {
                                        diemGiaoDich = -diemGiaoDich;
                                    }
                                    ajaxHelper(DMDoiTuongUri + 'HuyHD_UpdateDiem?idDoiTuong=' + item.ID_DoiTuong + '&diemGiaoDich=' + diemGiaoDich, 'POST').done(function (data) {
                                    });
                                }
                                objDiary.ID_HoaDon = idHoaDon;
                                objDiary.LoaiHoaDon = item.LoaiHoaDon;
                                objDiary.ThoiGianUpdateGV = item.NgayLapHoaDon;
                                Post_NhatKySuDung_UpdateGiaVon(objDiary);
                                vmThanhToan.NangNhomKhachHang(item.ID_DoiTuong);
                                if (item.LoaiHoaDon === 25) {
                                    HuyHoaDon_UpdateLichBaoDuong(idHoaDon);
                                }
                            }
                            else {
                                Insert_NhatKyThaoTac_1Param(objDiary);
                            }
                        },
                        error: function (error) {
                            ShowMessage_Danger('Cập nhật trạng thái thất bại');
                        },
                        complete: function () {
                            $('#wait').remove();
                            $('#modalPopuplgDelete').modal('hide');
                        }
                    });
                })
            }
        });
    }

    function HuyHoaDon_UpdateLichBaoDuong(idHoaDon) {
        if (!self.isLeeAuto()) {
            ajaxHelper('/api/DanhMuc/GaraAPI/' + 'HuyHoaDon_UpdateLichBaoDuong?idHoaDon=' + idHoaDon, 'GET').done(function (x) {
            });
        }
    }
    function UpdateLichBD_whenChangeNgayLapHD(idHoaDon, ngaylapOld, ngaylapNew) {
        if (!self.isLeeAuto()) {
            ajaxHelper('/api/DanhMuc/GaraAPI/UpdateLichBD_whenChangeNgayLapHD?idHoaDon=' + idHoaDon +
                '&ngaylapOld=' + ngaylapOld + '&ngaylapNew=' + ngaylapNew, 'GET')
                .done(function (x) {
                })
        }
    }

    // huy hdDoi, khong huy hdTra
    self.HuyHD_DoiTraHang = function (parent, item) {
        var idDoiTra = item.ID;

        var msg = 'Hóa đơn <b>' + item.MaHoaDon + ' </b> có liên quan đến giao dịch trả hàng <b>' + parent.MaHoaDon + ' </b> . Bạn có chắc chắn muốn hủy không?';
        dialogConfirm('Xác nhận hủy', msg, function () {
            $.ajax({
                type: "POST",
                url: BH_HoaDonUri + "Huy_HoaDon?id=" + idDoiTra + '&nguoiSua=' + userLogin + '&iddonvi=' + id_donvi,
                dataType: 'json',
                contentType: 'application/json',
                success: function (result) {
                    SearchHoaDon();

                    var diemGiaoDich = - item.DiemGiaoDich;
                    if (diemGiaoDich !== 0 && parent.ID_DoiTuong !== null) {
                        ajaxHelper(DMDoiTuongUri + 'HuyHD_UpdateDiem?idDoiTuong=' + parent.ID_DoiTuong + '&diemGiaoDich=' + diemGiaoDich, 'POST').done(function (data) {
                        });
                    }
                    // insert Ht_NhatKySuDung (HDDoiTra)
                    var objDiary = {
                        ID_NhanVien: _id_NhanVien,
                        ID_DonVi: id_donvi,
                        ChucNang: "Hóa đơn đổi trả",
                        NoiDung: "Xóa hóa đơn đổi trả: " + item.MaHoaDon,
                        NoiDungChiTiet: "Xóa hóa đơn đổi trả: " + item.MaHoaDon,
                        LoaiNhatKy: 3,
                        ID_HoaDon: idDoiTra,
                        LoaiHoaDon: 1,
                        ThoiGianUpdateGV: item.NgayLapHoaDon,
                    };
                    Post_NhatKySuDung_UpdateGiaVon(objDiary);
                    ShowMessage_Success("Hủy hóa đơn thành công");
                },
                error: function (error) {
                    ShowMessage_Danger('Cập nhật hóa đơn đổi trả trạng thái thất bại');
                },
                complete: function () {
                    $('#modalPopuplgDelete').modal('hide');
                }
            })
        });
    }

    self.GetID_NhanVien = function (item) {
        self.ID_NhanVieUpdateHD(item.ID_NhanVien); //--> get to do updateHoaDon
    }
    self.updateHoaDon = function (formElement) {
        var id = formElement.ID;
        var maHoaDon = formElement.MaHoaDon;
        var idNhanVien = self.ID_NhanVieUpdateHD();
        var ngaylapHDOld = formElement.NgayLapHoaDon;
        const loaiHoaDon = formElement.LoaiHoaDon;
        if (idNhanVien === undefined) {
            // if not change ID_NhanVien --> get from DB
            idNhanVien = formElement.ID_NhanVien;
            // if ID_NhanVien in DB = null --> get ID_NhanVien login
            if (idNhanVien === null) {
                idNhanVien = _id_NhanVien;
            }
        }
        if (self.NgayLapHD_Update() === undefined) {
            self.NgayLapHD_Update(moment(formElement.NgayLapHoaDon).format('DD/MM/YYYY HH:mm'));
        }
        var check = CheckNgayLapHD_format(self.NgayLapHD_Update(), formElement.ID_DonVi);
        if (!check) {
            return;
        }
        var ngaylapHD = moment(self.NgayLapHD_Update(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm:ss');
        var HoaDon = {
            ID: id,
            MaHoaDon: maHoaDon,
            ID_NhanVien: idNhanVien,
            DienGiai: formElement.DienGiai,
            NguoiSua: userLogin,
            NgayLapHoaDon: ngaylapHD,
        };
        // compare to update GiaVon (alway Ngay min)
        ngaylapHDOld = moment(ngaylapHDOld).format('YYYY-MM-DD HH:mm:ss'); // alway NgayLapHoaDon old (Tinh said 2019.06.20)
        //if (ngaylapHD < ngaylapHDOld) {
        //ngaylapHDOld = ngaylapHD;
        //}
        var myData = {};
        myData.id = id;
        myData.objNewHoaDon = HoaDon;
        $.ajax({
            data: myData,
            url: BH_HoaDonUri + "PutBH_HoaDon2",
            type: 'PUT',
            async: true,
            dataType: 'json',
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            success: function (x) {
                if (x.res === true) {
                    SearchHoaDon();
                    ShowMessage_Success("Cập nhật " + sLoai + " thành công")
                    var objDiary = {
                        ID_NhanVien: _id_NhanVien,
                        ID_DonVi: id_donvi,
                        ChucNang: 'Danh mục ' + sLoai,
                        NoiDung: "Cập nhật  " + sLoai + ": " + maHoaDon,
                        NoiDungChiTiet: "Cập nhật  " + sLoai + ": " + maHoaDon,
                        LoaiNhatKy: 2
                    };
                    if (loaiHoaDon !== 3 && loaiHoaDon !== 32) {
                        objDiary.ID_HoaDon = id;
                        objDiary.LoaiHoaDon = loaiHoaDon;
                        objDiary.ThoiGianUpdateGV = ngaylapHDOld;
                        Post_NhatKySuDung_UpdateGiaVon(objDiary);

                        if (loaiHoaDon === 25) {
                            UpdateLichBD_whenChangeNgayLapHD(id, ngaylapHDOld, ngaylapHD);
                        }
                    }
                    else {
                        Insert_NhatKyThaoTac_1Param(objDiary);
                    }
                }
                else {
                    ShowMessage_Danger("Cập nhật " + sLoai + " thất bại");
                }
            },
            error: function (jqXHR, textStatus, errorThrown) {
                self.error(textStatus + ": " + errorThrown + ": " + jqXHR.responseText);
            },
        })
    }
    self.exportToExcelDoiTuong = function () {
        tableToExcel('tblDanhMucHangHoa', 'dmHangHoas.xls');
    }
    self.importFromExcelDoiTuong = function () {
        $("#fileLoader").click();
    }

    $('#txtMaHD, #txtMaHDGoc').keypress(function (e) {
        $("#iconSort").remove();
        ResetColumnSort();
        if (e.keyCode === 13 || e.which === 13) {
            // reset currentPage if is finding at other page > 1
            self.currentPage(0);
            SearchHoaDon();
        }
    })
    self.Click_IconSearch = function () {
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    }
    function ResetColumnSort() {
        self.columsort('NgayLapHoaDon');
        self.sort(0);
    }
    //phân trang
    self.pageSizes = [10, 20, 30, 40, 50];
    self.pageSize = ko.observable(self.pageSizes[0]);
    self.currentPage = ko.observable(0);
    self.fromitem = ko.observable(0);
    self.toitem = ko.observable(0);
    self.PageCount = ko.observable(0);
    self.TotalRecord = ko.observable(0);
    // tim kiem JqAuto hàng hóa
    self.filterFind = function (item, inputString) {
        var itemSearch = locdau(item.TenHangHoa);
        var itemSearch1 = locdau(item.MaHangHoa);
        var locdauInput = locdau(inputString);
        var arr = itemSearch.split(/\s+/);
        var arr1 = itemSearch1.split(/\s+/);
        var sThreechars = '';
        var sThreechars1 = '';
        for (let i = 0; i < arr.length; i++) {
            sThreechars += arr[i].toString().split('')[0];
        }
        for (let i = 0; i < arr1.length; i++) {
            sThreechars1 += arr1[i].toString().split('')[0];
        }
        return itemSearch.indexOf(locdauInput) > -1 ||
            itemSearch1.indexOf(locdauInput) > -1 ||
            sThreechars.indexOf(locdauInput) > -1 ||
            sThreechars1.indexOf(locdauInput) > -1;
    }
    self.ResetCurrentPage = function () {
        $("#iconSort").remove();
        ResetColumnSort();

        self.currentPage(0);
        SearchHoaDon();
    };
    function getAllPhongBan() {
        ajaxHelper('/api/DanhMuc/DM_ViTriAPI/' + "GetListViTris", 'GET').done(function (data) {
            self.PhongBans(data);
        });
    }
    function getAllGiaBan() {
        ajaxHelper("/api/DanhMuc/DM_GiaBanAPI/" + "GetDM_GiaBanByIDDonVi?iddonvi=" + id_donvi, 'GET').done(function (data) {
            if (data !== null && data.length > 0) {
                self.GiaBans(data);
            }
            var objBGChung = {
                ID: '00000000-0000-0000-0000-000000000000',
                //ID: null,
                TenGiaBan: 'Bảng giá chung',
            }
            self.GiaBans.unshift(objBGChung);
        })
    }
    function getListNhanVien() {
        ajaxHelper("/api/DanhMuc/NS_NhanVienAPI/" + "GetNS_NhanVien_InforBasic?idDonVi=" + id_donvi, 'GET').done(function (data) {
            self.NhanViens(data);

            vmThanhToan.listData.NhanViens = self.NhanViens();
            vmHoaHongHoaDon.listData.NhanViens = self.NhanViens();
            vmHoaHongDV.listData.NhanViens = self.NhanViens();
            vmHoaHongDV.inforHoaDon.ID_DonVi = id_donvi;
        });
    }
    self.ShowColumn_LoHang = ko.observable(false);
    function GetCauHinhHeThong() {
        ajaxHelper('/api/DanhMuc/HT_ThietLapAPI/' + "GetCauHinhHeThong/" + id_donvi, 'GET').done(function (data) {
            self.ThietLap(data);
            self.ShowColumn_LoHang(data.LoHang);
        });
    }
    function getAllChiNhanh() {
        ajaxHelper('/api/DanhMuc/DM_DonViAPI/' + "GetListDonViByIDNguoiDung?idnhanvien=" + _id_NhanVien, 'GET').done(function (data) {
            var arrSortbyName = data.sort((a, b) => a.TenDonVi.localeCompare(b.TenDonVi, undefined, { caseFirst: "upper" }));
            self.ChiNhanhs(arrSortbyName);
            vmThanhToan.listData.ChiNhanhs = arrSortbyName;

            var obj = {
                ID: id_donvi,
                TenDonVi: $('#_txtTenDonVi').html()
            }
            // assign mangChiNhanh, and set check: avoid load douple list HoaDon
            self.MangNhomDV.push(obj);
            $('#selec-all-DV li').each(function () {
                if ($(this).attr('id') === id_donvi) {
                    $(this).find('.fa-check').remove();
                    $(this).append('<i class="fa fa-check check-after-li" style="display:block"></i>')
                }
            });
            $('#choose_TenDonVi input').remove();
        });
    }
    self.filterProvince = function (item, inputString) {
        var itemSearch = locdau(item.TenTinhThanh);
        var locdauInput = locdau(inputString);
        // nen bat chat khong cho nhap dau cach o MaKH
        var arr = itemSearch.split(/\s+/);
        var sThreechars = '';
        for (let i = 0; i < arr.length; i++) {
            sThreechars += arr[i].toString().split('')[0];
        }
        return itemSearch.indexOf(locdauInput) > -1 ||
            sThreechars.indexOf(locdauInput) > -1;
    }
    self.filterDistrict = function (item, inputString) {
        var itemSearch = locdau(item.TenQuanHuyen);
        var locdauInput = locdau(inputString);
        // nen bat chat khong cho nhap dau cach o MaKH
        var arr = itemSearch.split(/\s+/);
        var sThreechars = '';
        for (let i = 0; i < arr.length; i++) {
            sThreechars += arr[i].toString().split('')[0];
        }
        return itemSearch.indexOf(locdauInput) > -1 ||
            sThreechars.indexOf(locdauInput) > -1;
    }
    self.GetClassHD = function (page) {
        return ((page.pageNumber - 1) === self.currentPage()) ? "click" : "";
    };
    self.SelectedPB = function (item) {
        $("#iconSort").remove();
        ResetColumnSort();
        var arrIDPB = [];
        for (let i = 0; i < self.PhongBanChosed().length; i++) {
            if ($.inArray(self.PhongBanChosed()[i].ID, arrIDPB) === -1) {
                arrIDPB.push(self.PhongBanChosed()[i].ID);
            }
        }
        if ($.inArray(item.ID, arrIDPB) === -1) {
            self.PhongBanChosed.push(item);
        }
        $('#choose-PB input').remove();
        // add check after li
        $('#selec-all-PB li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('i').remove();
                $(this).append('<i class="fa fa-check check-after-li"></i>')
            }
        });
        self.currentPage(0);
        SearchHoaDon();
    }
    self.ClosePhongBan = function (item) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.PhongBanChosed.remove(item);
        if (self.PhongBanChosed().length === 0) {
            $('#choose-PB').append('<input type="text" class="dropdown form-control" placeholder="Chọn phòng/bàn">');
        }
        // remove check
        $('#selec-all-PB li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('i').remove();
            }
        });
        SearchHoaDon();
    }
    self.SelectedGiaBan = function (item) {
        $("#iconSort").remove();
        ResetColumnSort();
        var arrID_BangGia = [];
        for (let i = 0; i < self.GiaBanChosed().length; i++) {
            if ($.inArray(self.GiaBanChosed()[i].ID, arrID_BangGia) === -1) {
                arrID_BangGia.push(self.GiaBanChosed()[i].ID);
            }
        }
        if ($.inArray(item.ID, arrID_BangGia) === -1) {
            self.GiaBanChosed.push(item);
        }
        $('#choose-GB input').remove();
        // add check after li
        $('#selec-all-GB li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('i').remove();
                $(this).append('<i class="fa fa-check check-after-li"></i>')
            }
        });
        self.currentPage(0);
        SearchHoaDon();
    }
    self.CloseBangGia = function (item) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.GiaBanChosed.remove(item);
        if (self.GiaBanChosed().length === 0) {
            $('#choose-GB').append('<input type="text" class="dropdown form-control" placeholder="Chọn bảng giá">');
        }
        // remove check
        $('#selec-all-GB li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('i').remove();
            }
        });
        SearchHoaDon();
    }
    self.ChosePTThanhToan = function (item) {
        $("#iconSort").remove();
        ResetColumnSort();
        var arrIDPB = [];
        for (let i = 0; i < self.PThucChosed().length; i++) {
            if ($.inArray(self.PThucChosed()[i].ID, arrIDPB) === -1) {
                arrIDPB.push(self.PThucChosed()[i].ID);
            }
        }
        if ($.inArray(item.ID, arrIDPB) === -1) {
            self.PThucChosed.push(item);
        }
        $('#choose-PThuc input').remove();
        // add check after li
        $('#selec-pt-ThanhToan li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('i').remove();
                $(this).append('<i class="fa fa-check check-after-li"></i>')
            }
        });
        self.currentPage(0);
        SearchHoaDon();
    }
    self.ClosePhuongThuc = function (item) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.PThucChosed.remove(item);
        if (self.PThucChosed().length === 0) {
            $('#choose-PThuc').append('<input type="text" class="dropdown form-control" placeholder="Chọn phương thức">');
        }
        // remove check
        $('#selec-pt-ThanhToan li').each(function () {
            if ($(this).attr('id') === item.ID) {
                $(this).find('i').remove();
            }
        });
        SearchHoaDon();
    }
    $('.choseNgayTao li').on('click', function () {
        $("#iconSort").remove();
        ResetColumnSort();
        $('#txtNgayTao').val($(this).text());
        self.filterNgayLapHD_Quy($(this).val());
        self.currentPage(0);
        SearchHoaDon();
    });

    function GetColumHide(firsColumn = 0) {// hdban: colum 1= checkbox
        var cacheHideColumn2 = localStorage.getItem(Key_Form);
        if (cacheHideColumn2 !== null) {
            cacheHideColumn2 = JSON.parse(cacheHideColumn2);
            switch (Key_Form) {
                case 'KeyInvoices':
                    break;
                case 'KeyOrders':
                    break;
                case 'KeyReturns':
                    cacheHideColumn2 = cacheHideColumn2.filter(x => $.inArray(['email', 'khuvuc', 'phuongxa'], x) == -1);
                    break;
                case 25:
                    break;
            }
            var arrColumn = [];
            columnHide = '';
            var tdClass = $('#tb thead tr th');
            for (var i = 0; i < cacheHideColumn2.length; i++) {
                var itemFor = cacheHideColumn2[i];
                if (itemFor.Value !== undefined) {
                    $(tdClass).each(function (index) {
                        var className = $(this).attr('class');
                        if (className !== undefined && className.indexOf(itemFor.Value) > -1) {
                            // push if not exist
                            if ($.inArray(itemFor.Value, arrColumn) === -1) {
                                arrColumn.push(itemFor.Value);
                                columnHide += (index - firsColumn) + '_';
                            }
                        }
                    })
                }
            }
        }
        var lstColumn = columnHide.split('_');
        lstColumn = lstColumn.filter(x => x !== '');
        var lstAfter = [];
        switch (loaiHoaDon) {
            case 1:
            case 2:
            case 3:
            case 32:
            case 25:
            case 6:
                for (let i = 0; i < lstColumn.length; i++) {
                    lstAfter.push(formatNumberToFloat(lstColumn[i]));
                }
                break;
        }

        columnHide = '';
        for (var i = 0; i < lstAfter.length; i++) {
            columnHide += lstAfter[i].toString() + '_';
        }
    }

    var dayStart_Excel, dayEnd_Excel;// used to export many hoadon

    async function SearchHoaDon(isExport = false) {
        var arrDV = [];
        $('.line-right').height(0).css("margin-top", "0px");
        var maHDFind = localStorage.getItem('FindHD');
        if (maHDFind !== null) {
            self.filter(maHDFind);
            self.filterNgayLapHD('0');
            self.filterNgayLapHD_Quy(0);
        }
        var txtMaHDon = self.filter();
        var txtMaHDgoc = self.filterMaHDGoc(); // HD tra hang
        var arrIDPB = [];
        for (let i = 0; i < self.PhongBanChosed().length; i++) {
            arrIDPB.push(self.PhongBanChosed()[i].ID);
        }
        var arrIDBangGia = [];
        for (let i = 0; i < self.GiaBanChosed().length; i++) {
            arrIDBangGia.push(self.GiaBanChosed()[i].ID);
        }
        var ptThanhToan = $.map(self.PThucChosed(), function (x) { return x.ID });

        if (txtMaHDon === undefined) {
            txtMaHDon = "";
        }
        if (txtMaHDgoc === undefined) {
            txtMaHDgoc = "";
        }
        var sTenChiNhanhs = '';
        for (let i = 0; i < self.MangNhomDV().length; i++) {
            if ($.inArray(self.MangNhomDV()[i], arrDV) === -1) {
                arrDV.push(self.MangNhomDV()[i].ID);
                sTenChiNhanhs += self.MangNhomDV()[i].TenDonVi + ',';
            }
        }
        sTenChiNhanhs = Remove_LastComma(sTenChiNhanhs);// use when export excel
        self.MangIDDV(arrDV);
        // avoid error in Store procedure
        if (self.MangIDDV().length === 0) {
            self.MangIDDV([id_donvi]);
        }
        // trang thai hoadon
        var arrStatus = [];
        if (self.TT_DaDuyet()) {
            arrStatus.push('0');
        }
        if (self.TT_TamLuu()) {
            arrStatus.push('1');
        }
        if (self.TT_GiaoHang()) {
            arrStatus.push('2');
        }
        if (self.TT_HoanThanh()) {
            arrStatus.push('3');
        }
        if (self.TT_DaHuy()) {
            arrStatus.push('4');
        }

        // NgayLapHoaDon
        var _now = new Date();  //current date of week
        var dayStart = '';
        var dayEnd = '';
        if (self.filterNgayLapHD() === '0') {
            switch (self.filterNgayLapHD_Quy()) {
                case 0:
                    // all
                    self.TodayBC('Toàn thời gian');
                    dayStart = '2010-01-01';
                    dayEnd = moment(_now).add(1, 'days').format('YYYY-MM-DD');
                    break;
                case 1:
                    // hom nay
                    self.TodayBC('Hôm nay');
                    dayStart = moment(_now).format('YYYY-MM-DD');
                    dayEnd = moment(_now).add(1, 'days').format('YYYY-MM-DD');
                    break;
                case 2:
                    // hom qua
                    self.TodayBC('Hôm qua');
                    dayEnd = moment(_now).format('YYYY-MM-DD');
                    dayStart = moment(_now).subtract(1, 'days').format('YYYY-MM-DD');
                    break;
                case 3:
                    // tuan nay
                    self.TodayBC('Tuần này');
                    dayStart = moment().startOf('week').add('days', 1).format('YYYY-MM-DD');
                    dayEnd = moment().endOf('week').add(2, 'days').format('YYYY-MM-DD');
                    break;
                case 4:
                    // tuan truoc
                    self.TodayBC('Tuần trước');
                    dayStart = moment().weekday(-6).format('YYYY-MM-DD');
                    dayEnd = moment(dayStart, 'YYYY-MM-DD').add(7, 'days').format('YYYY-MM-DD'); // add day in moment.js
                    break;
                case 5:
                    // 7 ngay qua
                    self.TodayBC('7 ngày qua');
                    dayEnd = moment(_now).format('YYYY-MM-DD');
                    dayStart = moment(_now).subtract(7, 'days').format('YYYY-MM-DD');
                    break;
                case 6:
                    // thang nay
                    self.TodayBC('Tháng này');
                    dayStart = moment().startOf('month').format('YYYY-MM-DD');
                    dayEnd = moment().endOf('month').add(1, 'days').format('YYYY-MM-DD'); // add them 1 ngày 01-month-year --> compare in SQL
                    break;
                case 7:
                    // thang truoc
                    self.TodayBC('Tháng trước');
                    dayStart = moment().subtract(1, 'months').startOf('month').format('YYYY-MM-DD');
                    dayEnd = moment().subtract(1, 'months').endOf('month').add(1, 'days').format('YYYY-MM-DD');
                    break;
                case 10:
                    // quy nay
                    self.TodayBC('Quý này');
                    dayStart = moment().startOf('quarter').format('YYYY-MM-DD');
                    dayEnd = moment().endOf('quarter').add(1, 'days').format('YYYY-MM-DD');
                    break;
                case 11:
                    // quy truoc = currQuarter -1; // if (currQuarter -1 === 0) --> (assign = 1)
                    self.TodayBC('Quý trước');
                    var prevQuarter = moment().quarter() - 1;
                    if (prevQuarter === 0) {
                        // get quy 4 cua nam truoc 01/10/... -->  31/21/...
                        let prevYear = moment().year() - 1;
                        dayStart = prevYear + '-' + '10-01';
                        dayEnd = moment().year() + '-' + '01-01';
                    }
                    else {
                        dayStart = moment().quarter(prevQuarter).startOf('quarter').format('YYYY-MM-DD');
                        dayEnd = moment().quarter(prevQuarter).endOf('quarter').add(1, 'days').format('YYYY-MM-DD');
                    }
                    break;
                case 12:
                    // nam nay
                    self.TodayBC('Năm này');
                    dayStart = moment().startOf('year').format('YYYY-MM-DD');
                    dayEnd = moment().endOf('year').add(1, 'days').format('YYYY-MM-DD');
                    break;
                case 13:
                    // nam truoc
                    self.TodayBC('Năm trước');
                    var prevYear = moment().year() - 1;
                    dayStart = moment().year(prevYear).startOf('year').format('YYYY-MM-DD');
                    dayEnd = moment().year(prevYear).endOf('year').add(1, 'days').format('YYYY-MM-DD');
                    break;
            }
        }
        else {
            var arrDate = self.filterNgayLapHD_Input().split('-');
            dayStart = moment(arrDate[0], 'DD/MM/YYYY').format('YYYY-MM-DD');
            dayEnd = moment(arrDate[1], 'DD/MM/YYYY').add('days', 1).format('YYYY-MM-DD');

            self.TodayBC('Từ ' + moment(arrDate[0], 'DD/MM/YYYY').format('DD/MM/YYYY') + ' đến ' + moment(arrDate[1], 'DD/MM/YYYY').format('DD/MM/YYYY'));
        }
        dayStart_Excel = dayStart;
        dayEnd_Excel = dayEnd;
        var Params_GetListHoaDon = {
            CurrentPage: self.currentPage(),
            PageSize: self.pageSize(),
            LoaiHoaDon: loaiHoaDon,
            LaHoaDonSuaChua: [self.LoaiHoaDonMenu()],
            MaHoaDon: locdau(txtMaHDon).trim(),
            MaHoaDonGoc: locdau(txtMaHDgoc.trim()),
            ID_ChiNhanhs: self.MangIDDV(),
            ID_ViTris: arrIDPB,
            ID_BangGias: arrIDBangGia,
            ID_NhanViens: [_id_NhanVien], // nvlogin
            NguoiTao: userLogin,
            TrangThai: '',
            NgayTaoHD_TuNgay: dayStart,
            NgayTaoHD_DenNgay: dayEnd,
            TrangThai_SapXep: self.sort(), // 1. Tang dan, 2. Giamdan
            Cot_SapXep: self.columsort(),
            //PTThanhToan: '',
            ColumnsHide: '',
            SortBy: self.sort() == 1 ? 'ASC' : 'DESC',
            ValueText: sTenChiNhanhs,
            TrangThaiHDs: arrStatus,
            PhuongThucTTs: ptThanhToan,
            BaoHiem: self.BaoHiem()
        }

        if (isExport) {
            $('.content-table').gridLoader();
            var txtLoaiHD = 'Hóa đơn';
            var funcName = 'ExportExcel_HoaDonBanLe'; // loai 1, 19, 2
            var noidungNhatKy = "Xuất excel danh sách hóa đơn";
            var fileNameExport = 'DanhSachHoaDonBanLe.xlsx';

            switch (loaiHoaDon) {
                case 1:
                    GetColumHide(1);
                    funcName = 'ExportExcel_HoaDonBanLe';
                    fileNameExport = 'DanhSachHoaDonBanLe.xlsx';
                    break;
                case 25:
                    funcName = 'ExportExcel_HoaDonSuaChua';
                    fileNameExport = 'DanhSachHoaDonSuaChua.xlsx';
                    GetColumHide(1);
                    break;
                case 2:
                    GetColumHide(1);
                    txtLoaiHD = 'Hóa đơn bảo hành';
                    funcName = 'ExportExcel_HoaDonBaoHanh';
                    noidungNhatKy = "Xuất excel danh sách hóa đơn bảo hành";
                    break;
                case 3:
                    GetColumHide(0);
                    if (self.LoaiHoaDonMenu() === 0) {
                        funcName = 'ExportExcel_DatHang';
                        fileNameExport = 'HoaDonDatHang.xlsx';
                        txtLoaiHD = 'Đặt hàng';
                        noidungNhatKy = "Xuất excel danh sách hóa đơn đặt hàng";

                        // remove column {TongChiPhi}
                        let allHide = columnHide.split('_');
                        let arrNew = [];
                        for (let i = 0; i < allHide.length; i++) {
                            if (allHide[i] !== '') {
                                let col = formatNumberToFloat(allHide[i]);
                                if (col > 12) {
                                    arrNew.push(col + 1);
                                }
                                else {
                                    arrNew.push(col);
                                }
                            }
                        }
                        arrNew.push(13);
                        columnHide = arrNew.join('_');
                    }
                    else {
                        funcName = 'ExportExcel_BaoGiaSuaChua';
                        txtLoaiHD = 'Báo giá sửa chữa';
                        noidungNhatKy = "Xuất excel danh sách báo giá sửa chữa";
                        fileNameExport = 'DanhSachBaoGiaSuaChua.xlsx';

                        // remove column {TongChiPhi}
                        let allHide = columnHide.split('_');
                        let arrNew = [];
                        for (let i = 0; i < allHide.length; i++) {
                            if (allHide[i] !== '') {
                                let col = formatNumberToFloat(allHide[i]);
                                if (col > 14) {
                                    arrNew.push(col + 1);
                                }
                                else {
                                    arrNew.push(col);
                                }
                            }
                        }
                        arrNew.push(15);
                        columnHide = arrNew.join('_');
                    }
                    break;
                case 32:
                    GetColumHide(0);
                    funcName = 'ExportExcel_BaoGiaSuaChua';
                    txtLoaiHD = 'Lệnh bảo hành';
                    noidungNhatKy = "Xuất excel danh sách lệnh bảo hành";
                    fileNameExport = 'DanhSachBaoGiaSuaChua.xlsx';

                    // remove column {TongChiPhi}
                    let allHide = columnHide.split('_');
                    let arrNew = [];
                    for (let i = 0; i < allHide.length; i++) {
                        if (allHide[i] !== '') {
                            let col = formatNumberToFloat(allHide[i]);
                            if (col > 14) {
                                arrNew.push(col + 1);
                            }
                            else {
                                arrNew.push(col);
                            }
                        }
                    }
                    arrNew.push(15);
                    columnHide = arrNew.join('_');

                    break;
                case 6:
                    GetColumHide(0);
                    txtLoaiHD = 'Trả hàng';
                    funcName = 'ExportExcel_PhieuTraHang';
                    fileNameExport = 'PhieuTraHang.xlsx';
                    noidungNhatKy = "Xuất excel danh sách hóa đơn trả hàng";
                    break;
            }
            Params_GetListHoaDon.currentPage = 0;
            Params_GetListHoaDon.PageSize = self.TotalRecord();
            Params_GetListHoaDon.ColumnsHide = columnHide;

            let exportOK = false;
            exportOK = await commonStatisJs.DowloadFile_fromBrower(BH_HoaDonUri + funcName, 'POST', Params_GetListHoaDon, fileNameExport);
            if (exportOK) {
                $('.content-table').gridLoader({ show: false });
                commonStatisJs.ShowMessageSuccess("Xuất file thành công.");
                var objDiary = {
                    ID_NhanVien: _id_NhanVien,
                    ID_DonVi: id_donvi,
                    ChucNang: txtLoaiHD,
                    NoiDung: noidungNhatKy,
                    LoaiNhatKy: 6 // 1: Thêm mới, 2: Cập nhật, 3: Xóa, 4: Hủy, 5: Import, 6: Export, 7: Đăng nhập
                };
                Insert_NhatKyThaoTac_1Param(objDiary);
            } else {
                commonStatisJs.ShowMessageDanger("Có lỗi xảy ra trong quá trình tải dữ liệu. Vui lòng kiểm tra lại.");
            }

        }
        else {
            var hasPermission = false;
            switch (loaiHoaDon) {
                case 0:
                case 1:
                case 25:
                    hasPermission = self.RoleView_Invoice();
                    break;
                case 3:
                    hasPermission = self.RoleView_Order();
                    break;
                case 32:
                    hasPermission = self.RoleView_LenhBH();
                    break;
                case 6:
                    hasPermission = self.RoleView_Return();
                    break;
            }
            if (hasPermission) {
                $('.content-table').gridLoader();
                ajaxHelper(BH_HoaDonUri + 'GetListInvoice_Paging', 'POST', Params_GetListHoaDon).done(function (x) {
                    $('.content-table').gridLoader({ show: false });

                    if (x.res && x.dataSoure.length > 0) {
                        let first = x.dataSoure[0];

                        self.HoaDons(x.dataSoure);
                        self.TotalRecord(first.TotalRow);
                        self.PageCount(first.TotalPage);
                        self.TongTienHang(first.SumTongTienHang);
                        self.TongChiPhi(first.SumTongChiPhi);
                        self.TongThanhToan(first.SumTongThanhToan);
                        self.TongGiamGia(first.SumTongGiamGia);
                        self.TongGiamGiaKM(first.SumKhuyeMai_GiamGia);
                        self.TongKhachTra(first.SumKhachDaTra);

                        self.PhaiThanhToanBaoHiem(first.SumPhaiThanhToanBaoHiem);
                        self.BaoHiemDaTra(first.SumBaoHiemDaTra);
                        self.TienDatCoc(first.SumTienCoc);
                        self.TongTienDoiDiem(first.SumTienDoiDiem);
                        self.TongTienTheGTri(first.SumThuTuThe);
                        self.ThanhTienChuaCK(first.SumThanhTienChuaCK);
                        self.GiamGiaCT(first.SumGiamGiaCT);
                        self.TongPhaiTraKhach(first.TongPhaiTraKhach);

                        self.TongTienBHDuyet(first.SumTongTienBHDuyet);
                        self.KhauTruTheoVu(first.SumKhauTruTheoVu);
                        self.GiamTruBoiThuong(first.SumGiamTruBoiThuong);
                        self.BHThanhToanTruocThue(first.SumBHThanhToanTruocThue);
                        self.TongTienThueBaoHiem(first.SumTongTienThueBaoHiem);
                        self.TongGiamTruBaoHiem(first.SumGiamTruThanhToanBaoHiem);

                        self.TongTienMat(first.SumTienMat);
                        self.TongChuyenKhoan(first.SumChuyenKhoan);
                        self.TongPOS(first.SumPOS);
                        self.TongTienThue(first.SumTongTienThue);
                        self.TongGiaTriSDDV(first.TongGiaTriSDDV);

                        // sum in page (1 page)
                        let phaiTTSauTrahang = x.dataSoure.reduce(function (x, item) {
                            return x + item.TongTienHDTra;
                        }, 0);
                        let conno = x.dataSoure.reduce(function (x, item) {
                            return x + item.ConNo;
                        }, 0);

                        self.TongKhachNo(first.SumConNo);
                        self.TongNoKhach(conno);// trahang

                        switch (loaiHoaDon) {
                            case 6:
                                self.KhachCanTra(phaiTTSauTrahang);
                                break;
                            default:
                                self.KhachCanTra(first.SumPhaiThanhToan);
                                break;
                        }
                    }
                    else {
                        // if not data, reset 
                        self.HoaDons([]);
                        self.TotalRecord(0);
                        self.PageCount(0);
                        self.TongTienHang(0);
                        self.TongChiPhi(0);
                        self.TongThanhToan(0);
                        self.TongGiamGia(0);
                        self.GiamGiaCT(0);
                        self.TongGiamGiaKM(0);
                        self.TongKhachTra(0);
                        self.TongPhaiTraKhach(0);
                        self.TongKhachNo(0);
                        self.TongTienDoiDiem(0);
                        self.TongTienTheGTri(0);
                        self.KhachCanTra(0);
                        self.TongTienMat(0);
                        self.TongPOS(0);
                    }
                    HideShowColumn();
                    SetCheck_Input();
                });
            }
            localStorage.removeItem('FindHD');
        }
    }
    self.La_HDSuaChua.subscribe(function () {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.La_HDBan.subscribe(function () {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.TT_HoanThanh.subscribe(function (newVal) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.TT_DaHuy.subscribe(function (newVal) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.TT_TamLuu.subscribe(function (newVal) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.BaoHiemCo.subscribe(function (newVal) {
        self.CalcBaoHiem();
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.BaoHiemKhong.subscribe(function (newVal) {
        self.CalcBaoHiem();
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });

    self.CalcBaoHiem = function () {
        let intBaoHiemCo = 1;
        let intBaoHiemKhong = 2;
        if (self.BaoHiemCo() === true) {
            intBaoHiemCo = 1;
        }
        else {
            intBaoHiemCo = 0;
        }
        if (self.BaoHiemKhong() === true) {
            intBaoHiemKhong = 2;
        }
        else {
            intBaoHiemKhong = 0;
        }
        self.BaoHiem(intBaoHiemCo + intBaoHiemKhong);
    }

    self.TT_DaDuyet.subscribe(function (newVal) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.TT_GiaoHang.subscribe(function (newVal) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.filterNgayLapHD.subscribe(function (newVal) {
        $("#iconSort").remove();
        ResetColumnSort();
        self.currentPage(0);
        SearchHoaDon();
    });
    self.PageResults_HoaDon = ko.computed(function () {
        var first = self.currentPage() * self.pageSize();
        if (self.HoaDons() !== null) {
            return self.HoaDons().slice(first, first + self.pageSize());
        }
    });
    self.PageList_Display = ko.computed(function () {
        var arrPage = [];
        var allPage = self.PageCount();
        var currentPage = self.currentPage();
        if (allPage > 0) {
            if (allPage > 4) {
                var i = 0;
                if (currentPage === 0) {
                    i = parseInt(self.currentPage()) + 1;
                }
                else {
                    i = self.currentPage();
                }
                if (allPage >= 5 && currentPage > allPage - 5) {
                    if (currentPage >= allPage - 2) {
                        // get 5 trang cuoi cung
                        for (let i = allPage - 5; i < allPage; i++) {
                            let obj = {
                                pageNumber: i + 1,
                            };
                            arrPage.push(obj);
                        }
                    }
                    else {
                        // get currentPage - 2 , currentPage, currentPage + 2
                        if (currentPage === 1) {
                            for (let j = currentPage - 1; (j <= currentPage + 3) && j < allPage; j++) {
                                let obj = {
                                    pageNumber: j + 1,
                                };
                                arrPage.push(obj);
                            }
                        } else {
                            for (let j = currentPage - 2; (j <= currentPage + 2) && j < allPage; j++) {
                                let obj = {
                                    pageNumber: j + 1,
                                };
                                arrPage.push(obj);
                            }
                        }
                    }
                }
                else {
                    // get 5 trang dau
                    if (i >= 2) {
                        while (arrPage.length < 5) {
                            let obj = {
                                pageNumber: i - 1,
                            };
                            arrPage.push(obj);
                            i = i + 1;
                        }
                    }
                    else {
                        while (arrPage.length < 5) {
                            let obj = {
                                pageNumber: i,
                            };
                            arrPage.push(obj);
                            i = i + 1;
                        }
                    }
                }
            }
            else {
                // neu chi co 1 trang --> khong hien thi DS trang
                if (allPage > 1) {
                    for (let i = 0; i < allPage; i++) {
                        let obj = {
                            pageNumber: i + 1,
                        };
                        arrPage.push(obj);
                    }
                }
            }
            self.fromitem((self.currentPage() * self.pageSize()) + 1);
            if (((self.currentPage() + 1) * self.pageSize()) > self.HoaDons().length) {
                var fromItem = (self.currentPage() + 1) * self.pageSize();
                if (fromItem < self.TotalRecord()) {
                    self.toitem((self.currentPage() + 1) * self.pageSize());
                }
                else {
                    self.toitem(self.TotalRecord());
                }
            } else {
                self.toitem((self.currentPage() * self.pageSize()) + self.pageSize());
            }
        }
        return arrPage;
    });
    self.VisibleStartPage = ko.computed(function () {
        if (self.PageList_Display().length > 0) {
            return self.PageList_Display()[0].pageNumber !== 1;
        }
    });
    self.VisibleEndPage = ko.computed(function () {
        if (self.PageList_Display().length > 0) {
            return self.PageList_Display()[self.PageList_Display().length - 1].pageNumber !== self.PageCount();
        }
    });
    self.GoToPageHD = function (page) {
        if (page.pageNumber !== '.') {
            self.currentPage(page.pageNumber - 1);
            SearchHoaDon();
        }
    };
    function SetCheck_Input() {
        // find in list and set check
        var countCheck = 0;
        $('#tb tr td.check-group input').each(function (x) {
            var id = $(this).attr('id');
            if ($.inArray(id, arrIDCheck) > -1) {
                $(this).prop('checked', true);
                countCheck += 1;
            }
            else {
                $(this).prop('checked', false);
            }
        });
        // set again check header
        var ckHeader = $('#tb thead tr th:eq(0) input')
        if (countCheck === self.HoaDons().length) {
            ckHeader.prop('checked', true);
        }
        else {
            ckHeader.prop('checked', false);
        }
    }
    self.StartPage = function () {
        self.currentPage(0);
        SearchHoaDon();
    }
    self.BackPage = function () {
        if (self.currentPage() > 1) {
            self.currentPage(self.currentPage() - 1);
            SearchHoaDon();
        }
    }
    self.GoToNextPage = function () {
        if (self.currentPage() < self.PageCount() - 1) {
            self.currentPage(self.currentPage() + 1);
            SearchHoaDon();
        }
    }
    self.EndPage = function () {
        if (self.currentPage() < self.PageCount() - 1) {
            self.currentPage(self.PageCount() - 1);
            SearchHoaDon();
        }
    }
    //sort by cột trong bảng hóa đơn
    $('#tb thead tr').on('click', 'th', function () {
        var id = $(this).attr('class');
        if ($(this).hasClass('check-group')) {
            return;
        }
        switch (id) {
            case "mahoadon":
                self.columsort("MaHoaDon");
                break;
            case "ngaylaphoadon":
                self.columsort("NgayLapHoaDon");
                break;
            case "madathang":
                self.columsort("MaHoaDonGoc");
                break;
            case "makhachhang":
                self.columsort("MaKhachHang");
                break;
            case "tenkhachhang":
                self.columsort("TenKhachHang");
                break;
            case "email":
                self.columsort("Email");
                break;
            case "sodienthoai":
                self.columsort("SoDienThoai");
                break;
            case "diachi":
                self.columsort("DiaChi");
                break;
            case "khuvuc":
                self.columsort("KhuVuc");
                break;
            case "phuongxa":
                self.columsort("PhuongXa");
                break;
            case "nguoiban":
                self.columsort("NguoiBan");
                break;
            case "nguoitao":
                self.columsort("NguoiTao");
                break;
            case "ghichu":
                self.columsort("GhiChu");
                break;
            case "tongtienhang":
                self.columsort("TongTienHang");
                break;
            case "tonggiamgia":
                self.columsort("GiamGia");
                break;
            case "khachcantra":
                self.columsort("PhaiThanhToan");
                break;
            case "thanhtienchuack":
                self.columsort("ThanhTienChuaCK");
                break;
            case "giamgiact":
                self.columsort("GiamGiaCT");
                break;
            case "khachdatra":
                self.columsort("KhachDaTra");
                break;
            case "phitrahang":
                self.columsort("TongChiPhi");
                break;
            case "conno":
                self.columsort("ConNo");
                break;
            case "tienthue":
                self.columsort("VAT");
                break;
            case "tiendoidiem":
                self.columsort("TienDoiDiem");
                break;
            case "giatrisudung":
                self.columsort("GiaTriSDDV");
                break;
            case "tienmat":
                self.columsort("TienMat");
                break;
            case "chuyenkhoan":
                self.columsort("ChuyenKhoan");
                break;
            case "pos":
                self.columsort("TienATM");
                break;
            case "thegiatri":
                self.columsort("ThuTuThe");
                break;
            case "tiencoc":
                self.columsort("TienDatCoc");
                break;
            case "baohiemcantra":
                self.columsort("PhaiThanhToanBaoHiem");
                break;
            case "baohiemdatra":
                self.columsort("BaoHiemDaTra");
                break;
            case "tongtienBHduyet":
                self.columsort("TongTienBHDuyet");
                break;
            case "khautrutheovu":
                self.columsort("KhauTruTheoVu");
                break;
            case "giamtruboithuong":
                self.columsort("GiamTruBoiThuong");
                break;
            case "BHchitratruocVAT":
                self.columsort("BHThanhToanTruocThue");
                break;
            case "tongthueBH":
                self.columsort("TongTienThueBaoHiem");
                break;
            case "GiamTruBaoHiem":
                self.columsort("GiamTruThanhToanBaoHiem");
                break;
        }
        SortGrid(id);
    });
    function SortGrid(item) {
        $("#iconSort").remove();
        if (self.sort() === 1) {
            self.sort(2);
            $('#' + item).append(' ' + "<i id='iconSort' class='fa fa-caret-down' aria-hidden='true'></i>");
        }
        else {
            self.sort(1);
            $('#' + item).append(' ' + "<i id='iconSort' class='fa fa-caret-up' aria-hidden='true'></i>");
        }
        SearchHoaDon();
    };
    // Tra Hang
    self.gotoBanLe = function () {
        localStorage.setItem('fromTraHang', true);
        self.gotoGara();
    }

    self.headerTraHangs = [
        { title: 'Mã trả hàng', sortPropertyName: 'MaTraHang', asc: true, arrowDown: true, arrowUp: false, headerID: 'hdCodeTH' },
        { title: 'Mã hóa đơn', sortPropertyName: 'MaHoaDon', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdCode' },
        { title: 'Thời gian', sortPropertyName: 'NgayLapHoaDon', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdDate' },
        { title: 'Khách hàng', sortPropertyName: 'TenDoiTuong', asc: true, arrowDown: true, arrowUp: false, headerID: 'hdCusNam' },
        { title: 'Chi nhánh', sortPropertyName: '', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdBranch' },
        { title: 'Người trả nhận', sortPropertyName: '', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdPayer' },
        { title: 'Ghi chú', sortPropertyName: 'DienGiai', asc: true, arrowDown: true, arrowUp: false, headerID: 'hdNote' },
        { title: 'Tổng tiền hàng', sortPropertyName: 'TongTienHang', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdSum' },
        { title: 'Giảm giá', sortPropertyName: 'TongGiamGia', asc: true, arrowDown: true, arrowUp: false, headerID: 'hdSale' }, // branch: chi nhanh
        { title: 'Tổng sau giảm giá', sortPropertyName: '', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdSaleafter' },
        { title: 'Phí trả hàng', sortPropertyName: '', asc: true, arrowDown: true, arrowUp: false, headerID: 'hdFee' },
        { title: 'Cần trả khách', sortPropertyName: 'PhaiThanhToan', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdSumPay' },
        { title: 'Đã trả khách', sortPropertyName: 'DaThanhToan', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdPayed' },
        { title: 'Trạng thái', sortPropertyName: '', asc: true, arrowDown: false, arrowUp: false, headerID: 'hdStatus' },
    ];
    self.activeSortTH = self.headerTraHangs[1];
    self.sortHoaDon_TraHang = function (header, event) {
        if (self.activeSortTH === header) {
            header.asc = !header.asc;
            header.arrowDown = !header.arrowDown;
            header.arrowUp = !header.arrowUp;
        } else {
            self.activeSortTH.arrowDown = false;
            self.activeSortTH.arrowUp = false;
            self.activeSortTH = header;
            header.arrowDown = true;
        }
        if (header.arrowDown === true) {
            $('th[id^=hd]').each(function () {
                $(this).html($(this).text())
            });
            $('#' + header.headerID).html('');
            $('#' + header.headerID).html(header.title).append('<i class="fa fa-caret-down" aria-hidden="true"></i>');
        }
        else {
            $('th[id^=hd]').each(function () {
                $(this).html($(this).text())
            });
            $('#' + header.headerID).html('');
            $('#' + header.headerID).html(header.title).append('<i class="fa fa-caret-up" aria-hidden="true"></i>');
        }
        var prop = header.sortPropertyName;
        var ascSort = function (a, b) {
            if (typeof a[prop] === "number" || typeof a[prop] === "boolean") {
                return a[prop] < b[prop] ? -1 : a[prop] > b[prop] ? 1 : a[prop] === b[prop] ? 0 : 0;
            }
            else {
                if (a[prop] === null || a[prop] === undefined || b[prop] === null || b[prop] === undefined) {
                    if (a[prop] === null || b[prop] === null) {
                        // compare(null, string)= -1, compare(string, null)= 1
                        return (a[prop] === null && b[prop] !== null) ? -1 : (a[prop] !== null && b[prop] === null) ? 1 : 0;
                    }
                    else {
                        return a[prop] < b[prop] ? -1 : a[prop] > b[prop] ? 1 : a[prop] === b[prop] ? 0 : 0;
                    }
                }
                else {
                    return locdau(a[prop]) < locdau(b[prop]) ? -1 : locdau(a[prop]) > locdau(b[prop]) ? 1 : locdau(a[prop]) === locdau(b[prop]) ? 0 : 0;
                }
            }
        };
        var descSort = function (a, b) {
            if (typeof a[prop] === "number" || typeof a[prop] === "boolean") {
                return a[prop] > b[prop] ? -1 : a[prop] < b[prop] ? 1 : a[prop] === b[prop] ? 0 : 0;
            }
            else {
                if (a[prop] !== null && a[prop] !== undefined && b[prop] !== null && b[prop] !== undefined) {
                    return locdau(a[prop]) > locdau(b[prop]) ? -1 : locdau(a[prop]) < locdau(b[prop]) ? 1 : locdau(a[prop]) === locdau(b[prop]) ? 0 : 0;
                }
                else {
                    if (a[prop] === null || b[prop] === null) {
                        // compare(null, string)= 1, compare(string, null)= -1
                        return (a[prop] === null && b[prop] !== null) ? 1 : (a[prop] !== null && b[prop] === null) ? -1 : 0;
                    }
                    else {
                        return a[prop] > b[prop] ? -1 : a[prop] < b[prop] ? 1 : a[prop] === b[prop] ? 0 : 0;
                    }
                }
            }
        };
        var sortFunc = header.asc ? ascSort : descSort;
        self.HoaDons.sort(sortFunc);
    };
    self.gotoHoaDonGoc = function (item) {
        localStorage.setItem('FindHD', item.MaHoaDonGoc);
        switch (item.LoaiHoaDonGoc) {
            case 1:
                window.open('/#/Invoices', '_blank');
                break;
            case 2:
                window.location.href = '/#/HoaDonBaoHanh';
                break;
            case 19:
                window.open('/#/ServicePackage', '_blank');
                break;
            case 25:
                window.open('/#/HoaDonSuaChua', '_blank');
                break;
        }
    }
    self.gotoSoQuy = function (item) {
        localStorage.setItem('FindMaPhieuChi', item.MaPhieuChi);
        window.location.href = '/#/CashFlow';
    }
    self.gotoHoaDonTH = function (item) {
        switch (loaiHoaDon) {
            case 1:
                localStorage.setItem('FindHD', item.MaHoaDonGoc);
                window.location.href = '/#/Returns';
                break;
            case 6:
                // if click from HDDoiTra --> get MaHDparent
                localStorage.setItem('FindHD', self.MaHoaDonParent());
                window.location.href = '/#/Returns';
                break;
        }
    }
    self.gotoKhachHang = function (item, type) {
        switch (type) {
            case 1:
                localStorage.setItem('FindKhachHang', item.MaDoiTuong);
                window.location.href = '/#/Customers';
                break;
            case 2:
                if (!commonStatisJs.CheckNull(item.MaPhieuTiepNhan)) {
                    window.open('/#/DanhSachPhieuTiepNhan?' + item.MaPhieuTiepNhan, '_blank');
                }
                else {
                    self.LoadChiTietHD(item);
                }
                break;
            case 3:
                if (!commonStatisJs.CheckNull(item.BienSo)) {
                    window.open('/#/DanhSachXe?' + item.BienSo, '_blank');
                }
                else {
                    self.LoadChiTietHD(item);
                }
                break;
        }
    }
    //Trinhpv xuất excel HoaDon
    self.ColumnsExcel = ko.observableArray();
    self.addColum = function (item) {
        if (self.ColumnsExcel().length < 1) {
            self.ColumnsExcel.push(item);
        }
        else {
            for (let i = 0; i < self.ColumnsExcel().length; i++) {
                if (self.ColumnsExcel()[i] === item) {
                    self.ColumnsExcel.splice(i, 1);
                    break;
                }
                if (i === self.ColumnsExcel().length - 1) {
                    self.ColumnsExcel.push(item);
                    break;
                }
            }
        }
        self.ColumnsExcel.sort();
    }
    //===============================
    // triger khi đặt hàng thành công
    // đặt hàng
    //===============================
    $("body").on('ChangeDatHang', function () {
        SearchHoaDon();
    });
    //===============================
    // triger khi đặt hàng thành công
    // Hóa đơn
    //===============================
    $("body").on('ChangeHoaDon', function () {
        SearchHoaDon();
    });
    //===============================
    // triger khi đặt hàng thành công
    // TraHang
    //===============================
    $("body").on('ChangeTraHang', function () {
        SearchHoaDon();
    });
    var columnHide = null;
    self.loadColumnsHide = function () {
        //columnHide = null;
        //for (let i = 0; i < self.ColumnsExcel().length; i++) {
        //    if (i === 0) {
        //        columnHide = self.ColumnsExcel()[i];
        //    }
        //    else {
        //        columnHide = self.ColumnsExcel()[i] + "_" + columnHide;
        //    }
        //}
    }
    self.DownloadFileTeamplateXLSX = function (pathFile) {
        var url = "/api/DanhMuc/DM_HangHoaAPI/Download_fileExcel?fileSave=" + pathFile;
        window.location.href = url;
    }
    //xuất excel hóa đơn
    self.ExportMany_HD = function () {
        var arrDV = [];
        var sTenChiNhanhs = '';
        for (let i = 0; i < self.MangNhomDV().length; i++) {
            if ($.inArray(self.MangNhomDV()[i], arrDV) === -1) {
                arrDV.push(self.MangNhomDV()[i].ID);
                sTenChiNhanhs += self.MangNhomDV()[i].TenDonVi + ',';
            }
        }
        sTenChiNhanhs = sTenChiNhanhs.substr(0, sTenChiNhanhs.length - 1);

        ajaxHelper(BH_HoaDonUri + 'GetListHDbyIDs?lstID=' + arrIDCheck, 'POST', arrIDCheck).done(function (x) {
            if (x.res) {
                GetColumHide(1);
                let myData = {
                    LstExport: x.lstHD,
                    LoaiHoaDon: loaiHoaDon,
                    DayStart: dayStart_Excel,
                    DayEnd: dayEnd_Excel,
                    ColumnsHide: columnHide,
                    ChiNhanhs: sTenChiNhanhs,
                }

                ajaxHelper(BH_HoaDonUri + 'XuatFileHD_TongQuan', 'POST', myData).done(function (url) {
                    if (url !== "") {
                        self.DownloadFileTeamplateXLSX(url);
                        var objDiary = {
                            ID_NhanVien: _id_NhanVien,
                            ID_DonVi: id_donvi,
                            ChucNang: "Hóa đơn",
                            NoiDung: "Xuất file tổng quan danh sách hóa đơn ",
                            NoiDungChiTiet: "Xuất file danh sách hóa đơn gồm " + $.map(x.lstHD, function (x) { return x.MaHoaDon }).toString(),
                            LoaiNhatKy: 6 // 1: Thêm mới, 2: Cập nhật, 3: Xóa, 4: Hủy, 5: Import, 6: Export, 7: Đăng nhập
                        };
                        Insert_NhatKyThaoTac_1Param(objDiary)
                    }
                })
            }
        })
    }

    self.ExportExcel_HoaDon = function () {
        SearchHoaDon(true);
    }
    self.ExportExcel_ChiTietHoaDon = async function (item) {
        //let table = $('#home_' + item.ID);
        //    TableToExcel.convert(table[0], { // html code may contain multiple tables so here we are refering to 1st table tag
        //        name: `export.xlsx`, // fileName you could use any name
        //        sheet: {
        //            name: 'Sheet 1' // sheetName
        //        }
        //    });


        let exportOK = false;
        exportOK = await commonStatisJs.DowloadFile_fromBrower(BH_HoaDonUri + 'ExportExcel__ChiTietHoaDon?ID_HoaDon=' + item.ID + '&loaiHoaDon=' + loaiHoaDon + '&columHides=' + columnHide, 'GET', null, "GiaoDichHoaDon_ChiTiet.xlsx");
        if (exportOK) {
            commonStatisJs.ShowMessageSuccess("Xuất file thành công.");
            var objDiary = {
                ID_NhanVien: _id_NhanVien,
                ID_DonVi: id_donvi,
                ChucNang: "Hóa đơn",
                NoiDung: "Xuất báo cáo hóa đơn chi tiết theo mã: " + item.MaHoaDon,
                LoaiNhatKy: 6 // 1: Thêm mới, 2: Cập nhật, 3: Xóa, 4: Hủy, 5: Import, 6: Export, 7: Đăng nhập
            };
            Insert_NhatKyThaoTac_1Param(objDiary);
        } else {
            commonStatisJs.ShowMessageDanger("Có lỗi xảy ra trong quá trình tải dữ liệu. Vui lòng kiểm tra lại.");
        }

    }
    // xuất excel phiếu trả hàng
    self.ExportExcel_PhieuTraHang = async function () {
        await SearchHoaDon(true);
    }
    self.ExportExcel_ChiTietPhieuTraHang = async function (item) {

        let exportOK = false;
        exportOK = await commonStatisJs.DowloadFile_fromBrower(BH_HoaDonUri + 'ExportExcel__ChiTietPhieuTraHang?ID_HoaDon=' + item.ID, 'GET', null, "PhieuTraHang_ChiTiet.xlsx");
        if (exportOK) {
            commonStatisJs.ShowMessageSuccess("Xuất file thành công.");
            var objDiary = {
                ID_NhanVien: _id_NhanVien,
                ID_DonVi: id_donvi,
                ChucNang: "Trả hàng",
                NoiDung: "Xuất báo cáo phiếu trả hàng chi tiết theo mã: " + item.MaHoaDon,
                LoaiNhatKy: 6 // 1: Thêm mới, 2: Cập nhật, 3: Xóa, 4: Hủy, 5: Import, 6: Export, 7: Đăng nhập
            };
            Insert_NhatKyThaoTac_1Param(objDiary);
        } else {
            commonStatisJs.ShowMessageDanger("Có lỗi xảy ra trong quá trình tải dữ liệu. Vui lòng kiểm tra lại.");
        }
    }
    //xuất excel phiếu đặt hàng
    self.ExportExcel_DatHang = async function () {
        await SearchHoaDon(true);
    }
    self.ExportExcel_ChiTietPhieuDatHang = async function (item) {
        var objDiary = {
            ID_NhanVien: _id_NhanVien,
            ID_DonVi: id_donvi,
            ChucNang: "Đặt hàng",
            NoiDung: "Xuất báo cáo phiếu đặt hàng chi tiết theo mã: " + item.MaHoaDon,
            LoaiNhatKy: 6 // 1: Thêm mới, 2: Cập nhật, 3: Xóa, 4: Hủy, 5: Import, 6: Export, 7: Đăng nhập
        };

        let exportOK = false;
        exportOK = await commonStatisJs.DowloadFile_fromBrower(BH_HoaDonUri + 'ExportExcel_ChiTietPhieuDatHang?ID_HoaDon=' + item.ID, 'GET', null, "PhieuDatHang_ChiTiet.xlsx");
        if (exportOK) {
            commonStatisJs.ShowMessageSuccess("Xuất file thành công.");
            Insert_NhatKyThaoTac_1Param(objDiary);

        } else {
            commonStatisJs.ShowMessageDanger("Có lỗi xảy ra trong quá trình tải dữ liệu. Vui lòng kiểm tra lại.");
        }
    }

    async function GetInforCar_ByID(idXe) {
        if (!commonStatisJs.CheckNull(idXe)) {
            let xx = $.getJSON('/api/DanhMuc/GaraAPI/GetInforCar_ByID?id=' + idXe).done().then(function (x) {
                if (x.res) {
                    if (x.dataSoure.length > 0) {
                        return x.dataSoure[0];
                    }
                    return {}
                }
                return {}
            })
            return xx;
        }
    }
    // use enable/disable txtNgayLapHD, dropdown NVien
    self.ThayDoi_NgayLapHD = ko.observable(false);
    self.ThayDoi_NVienBan = ko.observable(false);
    self.RdoKhauTru = ko.observable(0);
    self.RdoCheTai = ko.observable(0);
    self.InVoiceChosing = ko.observable();

    function Caculator_ChiTietHD(cthdDB = []) {
        let tongsoluong = cthdDB.reduce(function (x, item) {
            return x + item.SoLuong;
        }, 0);
        let tonggiamgiahang = cthdDB.reduce(function (x, item) {
            return x + (item.TienChietKhau * item.SoLuong);
        }, 0);
        let tongtienhangchuaCK = cthdDB.reduce(function (x, item) {
            return x + (item.SoLuong * item.DonGia);
        }, 0);
        let arrHH = cthdDB.filter(x => x.LaHangHoa);
        let arrDV = cthdDB.filter(x => x.LaHangHoa === false);

        let tongDV = 0, tongDV_truocVAT = 0, tongDV_truocCK = 0;
        let tongHH = 0, tongHH_truocVAT = 0, tongHH_truocCK = 0;
        let DV_tongthue = 0, DV_tongCK = 0, DV_tongSL = 0;
        let HH_tongthue = 0, HH_tongCK = 0, HH_tongSL = 0;
        for (let k = 0; k < arrHH.length; k++) {
            let itFor = arrHH[k];
            let soluong = formatNumberToFloat(itFor.SoLuong);
            HH_tongSL += soluong;
            tongHH += formatNumberToFloat(itFor.ThanhToan);
            tongHH_truocVAT += RoundDecimal(formatNumberToFloat(itFor.ThanhTien), 3);
            tongHH_truocCK += soluong * formatNumberToFloat(itFor.DonGia);
            HH_tongthue += soluong * formatNumberToFloat(itFor.TienThue);
            HH_tongCK += soluong * formatNumberToFloat(itFor.TienChietKhau);
        }
        for (let k = 0; k < arrDV.length; k++) {
            let itFor = arrDV[k];
            let soluong = formatNumberToFloat(itFor.SoLuong);
            DV_tongSL += soluong;
            tongDV += formatNumberToFloat(itFor.ThanhToan);
            tongDV_truocVAT += RoundDecimal(formatNumberToFloat(itFor.ThanhTien), 3);
            tongDV_truocCK += soluong * formatNumberToFloat(itFor.DonGia);
            DV_tongthue += soluong * formatNumberToFloat(itFor.TienThue);
            DV_tongCK += soluong * formatNumberToFloat(itFor.TienChietKhau);
        }

        self.TongSoLuongHang(tongsoluong);
        self.TongGiamGiaHang(tonggiamgiahang);
        self.TongTienHangChuaCK(RoundDecimal(tongtienhangchuaCK, 3));

        self.TongTienPhuTung(tongHH);
        self.TongTienDichVu(tongDV);
        self.TongTienPhuTung_TruocCK(tongHH_truocCK);
        self.TongTienDichVu_TruocCK(tongDV_truocCK);
        self.TongTienPhuTung_TruocVAT(tongHH_truocVAT);
        self.TongTienDichVu_TruocVAT(tongDV_truocVAT);

        self.TongThue_PhuTung(HH_tongthue);
        self.TongCK_PhuTung(HH_tongCK);
        self.TongThue_DichVu(DV_tongthue);
        self.TongCK_DichVu(DV_tongCK);
        self.TongSL_DichVu(DV_tongSL);
        self.TongSL_PhuTung(HH_tongSL);
    }
    self.LoadChiTietHD = function (item, e) {
        self.InVoiceChosing(item);
        self.Enable_NgayLapHD(item.ChoThanhToan === null || !VHeader.CheckKhoaSo(moment(item.NgayLapHoaDon).format('YYYY-MM-DD'), item.ID_DonVi));

        self.currentPage_CTHD(0);
        var congthucBH = item.CongThucBaoHiem;

        if (commonStatisJs.CheckNull(congthucBH)) {
            congthucBH = '0';
        }
        congthucBH = congthucBH.toString().split('');
        if (congthucBH.length > 1) {
            self.RdoKhauTru(parseInt(congthucBH[0]));
            self.RdoCheTai(parseInt(congthucBH[1]));
        }
        else {
            self.RdoKhauTru(0);
            self.RdoCheTai(0);
        }

        // reset tab & set default active tab 1
        var thisObj = event.currentTarget;
        var ulTab = '';
        if (loaiHoaDon === 1 || loaiHoaDon === 0) {
            ulTab = $(thisObj).parent().next().find('.op-object-detail.nav-tabs');
        }
        else {
            ulTab = $(thisObj).next().find('.op-object-detail.nav-tabs');
        }
        ulTab.children('li').removeClass('active');
        ulTab.children('li').eq(0).addClass('active');
        // active tabcontent
        ulTab.next().children('.tab-pane').removeClass('active');
        ulTab.next().children('.tab-pane:eq(0)').addClass('active');
        self.NgayLapHD_Update(undefined);
        self.filterHangHoa_ChiTietHD(undefined);

        $('.txtNgayLapHD').datetimepicker({
            timepicker: true,
            mask: true,
            format: 'd/m/Y H:i',
            maxDate: new Date(),
            onChangeDateTime: function (dp, $input) {
                self.NgayLapHD_Update($input.val());
                CheckNgayLapHD_format(self.NgayLapHD_Update(), item.ID_DonVi);
            }
        });
        var $thiss = $(event.currentTarget).closest('tr').next().find('td').find('.op-object-detail').find('.loadcthd');
        var css = $(event.currentTarget).closest('tr').next().css('display');
        $(".op-js-tr-hide").hide();
        if (css === 'none') {
            $(event.currentTarget).closest('tr').next().toggle();
        }
        $thiss.gridLoader();
        self.BH_HoaDonChiTiets([]);

        ajaxHelper(BH_HoaDonUri + 'SP_GetChiTietHD_byIDHoaDon_ChietKhauNV?idHoaDon=' + item.ID, 'GET').done(function (data) {
            $thiss.gridLoader({ show: false });
            if (data !== null) {
                var sluong = 0;
                for (let i = 0; i < data.length; i++) {
                    sluong += data[i].SoLuong;
                    if (data[i].MaHangHoa.indexOf('{DEL}') > -1) {
                        data[i].MaHangHoa = data[i].MaHangHoa.substr(0, data[i].MaHangHoa.length - 5);
                        data[i].Del = '{Xóa}';
                    } else {
                        data[i].Del = "";
                    }
                    data[i] = AssignNVThucHien_toCTHD(data[i]);
                }
                self.BH_HoaDonChiTiets(data);
                self.TongSLuong(sluong);
            }
            else {
                self.BH_HoaDonChiTiets(data);
            }
        });
        var roleInsertQuy = $.grep(self.Quyen_NguoiDung(), function (x) {
            return x.MaQuyen.indexOf('SoQuy_ThemMoi') > -1;
        });

        vmThanhToan.GetSoDuTheGiaTri(item.ID_DoiTuong);// used to get print

        switch (loaiHoaDon) {
            case 0:
            case 1:
            case 25:
                // khong lam gi ca
                break;
            case 3:
                GetLichSuThanhToan_ofDatHang(item.ID);

                var roleXuLiDH = $.grep(self.Quyen_NguoiDung(), function (x) {
                    return x.MaQuyen.indexOf('DatHang_TaoHoaDon') > -1;
                });
                var roleInsert_Invoice = $.grep(self.Quyen_NguoiDung(), function (x) {
                    return x.MaQuyen.indexOf('HoaDon_ThemMoi') > -1;
                });
                if (roleInsert_Invoice.length > 0 && roleXuLiDH.length > 0) {
                    if (item.YeuCau === '1' || item.YeuCau === '2' || item.YeuCau === '') {
                        self.Show_BtnXulyDH(true);
                    }
                    else {
                        self.Show_BtnXulyDH(false);
                    }
                }
                break;
            case 32:
                GetLichSuThanhToan_ofDatHang(item.ID);

                var roleXuLiLBH = $.grep(self.Quyen_NguoiDung(), function (x) {
                    return x.MaQuyen.indexOf('LenhBaoHanh_TaoHoaDon') > -1;
                });
                var roleInsert_Invoice = $.grep(self.Quyen_NguoiDung(), function (x) {
                    return x.MaQuyen.indexOf('HoaDon_ThemMoi') > -1;
                });
                if (roleInsert_Invoice.length > 0 && roleXuLiLBH.length > 0) {
                    if (item.YeuCau === '1' || item.YeuCau === '2' || item.YeuCau === '') {
                        self.Show_BtnXulyDH(true);
                    }
                    else {
                        self.Show_BtnXulyDH(false);
                    }
                }
                break;
            case 6:
                self.MaHoaDonParent(item.MaHoaDon); // get MaHoaDon Parent --> go to gotoHoaDonTH (go to itself)
                GetLichSuThanhToan(item.ID, null);
                break;
        }
        GetInfor_PhieuTiepNhan(item.ID_PhieuTiepNhan);
    }

    async function GetChiTietHD_fromDB(idHoaDon) {
        const xx = await ajaxHelper(BH_HoaDonUri + 'SP_GetChiTietHD_byIDHoaDon_ChietKhauNV?idHoaDon=' + idHoaDon, 'GET').done()
            .then(function (data) {
                if (data != null) return data;
                return [];
            });
        return xx;
    }

    function CheckQuyen_HoaDonMua() {
        self.RoleUpdate_Invoice(CheckQuyenExist('HoaDon_CapNhat'));
        self.ThayDoi_NgayLapHD(CheckQuyenExist('HoaDon_ThayDoiThoiGian'));
        self.ThayDoi_NVienBan(CheckQuyenExist('HoaDon_ThayDoiNhanVien'));
        self.RoleExport_Invoice(CheckQuyenExist('HoaDon_XuatFile'));
        self.RoleDelete_Invoice(CheckQuyenExist('HoaDon_Xoa'));
        self.Show_BtnExcelDetail(self.RoleExport_Invoice());
    }

    self.GetLichSuThanhToan = function (item) {
        GetLichSuThanhToan(item.ID, item.ID_HoaDon);
    }
    self.GetLichSuTraHang = function () {
        GetLichSuTraHang(item.ID);
    }

    self.Enable_NgayLapHD = ko.observable(true);

    function CheckNgayLapHD_format(valDate, idDonVi = null) {
        if (idDonVi === null) {
            idDonVi = VHeader.IdDonVi;
        }
        var dateNow = moment(new Date()).format('YYYY-MM-DD HH:mm');
        var ngayLapHD = moment(valDate, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm');
        if (valDate === '') {
            ShowMessage_Danger("Vui lòng nhập ngày lập " + sLoai);
            return false;
        }
        if (valDate.indexOf('_') > -1) {
            ShowMessage_Danger("Ngày lập " + sLoai + ' chưa đúng định dạng');
            return false;
        }
        if (ngayLapHD > dateNow) {
            ShowMessage_Danger("Ngày lập " + sLoai + ' vượt quá thời gian hiện tại');
            return false;
        }
        let chotSo = VHeader.CheckKhoaSo(moment(ngayLapHD, 'YYYY-MM-DD HH:mm').format('YYYY-MM-DD'), idDonVi);
        if (chotSo) {
            ShowMessage_Danger(VHeader.warning.ChotSo.Update);
            return false;
        }
        return true;
    }

    function GetLichSuThanhToan(idHoaDon, idParent) {
        // load data from Quy_HoaDon
        ajaxHelper(Quy_HoaDonUri + 'GetQuyHoaDon_byIDHoaDon?idHoaDon=' + idHoaDon + '&idHoaDonParent=' + idParent, 'GET').done(function (data) {
            self.LichSuThanhToan(data);
        });
    }
    function GetLichSuTraHang(idHoaDon) {
        ajaxHelper(BH_HoaDonUri + 'GetHDTraHang_byIDHoaDon?idHoaDon=' + idHoaDon, 'GET').done(function (data) {
            self.LichSuTraHang(data); // = L/su HoaDon of HD Dathang
        });
    }
    function GetLichSuThanhToan_ofDatHang(id) {
        ajaxHelper(Quy_HoaDonUri + 'GetLichSuThanhToan_ofDatHang?id=' + id, 'GET').done(function (data) {
            self.LichSuThanhToanDH(data);
        });
    }
    function GetInfor_ofHDDoiTra(item) {
        // reset HoaDonDoiTra: if data null
        self.HoaDonDoiTra([]);
        ajaxHelper(BH_HoaDonUri + 'GetHoaDon_ByID?id=' + item.ID, 'GET').done(function (data) {
            // show hdDoiTra if chua huy
            if (data !== null && data.ChoThanhToan !== null) {
                var cthd = data.BH_HoaDon_ChiTiet;
                // remove tp dinh luong in CTHD
                data.BH_HoaDon_ChiTiet = $.grep(cthd, function (x) {
                    return x.ID_ChiTietDinhLuong === x.ID || x.ID_ChiTietDinhLuong === null;
                })
                self.HoaDonDoiTra(data);

                // vmThanhPhanCombo.GetAllCombo_byIDHoaDon(data.ID);
            }
            else {
                self.HoaDonDoiTra([]);
            }
        });
    }
    self.VisibleHisTraHang = ko.computed(function () {
        if (self.LichSuTraHang() === null) {
            return false;
        }
        else {
            return self.LichSuTraHang().length > 0;
        }
    })
    self.VisibleHis_HisHDofDH = ko.computed(function () {
        if (self.LichSuThanhToanDH() === null) {
            return false;
        }
        else {
            var count = 0;
            for (let i = 0; i < self.LichSuThanhToanDH().length; i++) {
                if (self.LichSuThanhToanDH()[i].LoaiHoaDon === 1) {
                    // Da tao HD from Dat hang
                    count += 1;
                }
            }
            if (count === 0) {
                return false;
            }
            else {
                return true;
            }
        }
    })
    self.VisibleHuyHD = ko.computed(function () {
        // if have TraHang --> hide btnHuyHoaDon
        if (self.LichSuTraHang() === null) {
            return true;
        }
        else {
            return false;
        }
    })
    self.VisibleHDDoiTra = ko.computed(function () {
        if (self.HoaDonDoiTra() === null) {
            return false;
        }
        else {
            return true;
        }
    })
    self.arrFilterBangGia = ko.computed(function () {
        var _filter = self.filterBangGia();
        return arrFilter = ko.utils.arrayFilter(self.GiaBans(), function (prod) {
            var chon = true;
            var arr = locdau(prod.TenGiaBan).split(/\s+/);
            var sSearch = '';
            for (let i = 0; i < arr.length; i++) {
                sSearch += arr[i].toString().split('')[0];
            }
            if (chon && _filter) {
                chon = (locdau(prod.TenGiaBan).indexOf(locdau(_filter)) >= 0 ||
                    sSearch.indexOf(locdau(_filter)) >= 0
                );
            }
            return chon;
        });
    });
    self.arrFilterPhongBan = ko.computed(function () {
        var _filter = self.filterPhongBan();
        return arrFilter = ko.utils.arrayFilter(self.PhongBans(), function (prod) {
            var chon = true;
            var arr = locdau(prod.TenViTri).split(/\s+/);
            var sSearch = '';
            for (let i = 0; i < arr.length; i++) {
                sSearch += arr[i].toString().split('')[0];
            }
            if (chon && _filter) {
                chon = (locdau(prod.TenViTri).indexOf(locdau(_filter)) >= 0 ||
                    sSearch.indexOf(locdau(_filter)) >= 0
                );
            }
            return chon;
        });
    });

    async function GetListHDbyIDs() {
        let yy = ajaxHelper(BH_HoaDonUri + 'GetListHDbyIDs?lstID=' + arrIDCheck, 'POST', arrIDCheck).done(function (x) {
        }).then(function (x) {
            if (x.res) {
                return x;
            }
            return {};
        })
        return yy;
    }

    async function GetContent_MauInMacDinh(maLoaiChungTu) {
        let yy = ajaxHelper('/api/DanhMuc/ThietLapApi/GetContent_MauInMacDinh?maChungTu=' + maLoaiChungTu
            + '&idDonVi=' + VHeader.IdDonVi, 'GET').done(function (x) {
            }).then(function (x) {
                return x;
            })
        return yy;
    }
    async function GetNoiDungMauIn_ById(idMauIn) {
        let yy = ajaxHelper('/api/DanhMuc/ThietLapApi/GetNoiDungMauIn_ById?idMauIn=' + idMauIn, 'GET').done(function (x) {
        }).then(function (x) {
            return x;
        })
        return yy;
    }

    function ConvertNumber_toRoman(num) {
        if (!+num)
            return false;
        var digits = String(+num).split(""),
            key = ["", "C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM",
                "", "X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC",
                "", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"],
            roman = "",
            i = 3;
        while (i--)
            roman = (key[+digits.pop() + (i * 10)] || "") + roman;
        return Array(+digits.join("") + 1).join("M") + roman;
    }

    function GroupCTHD_byNhomHang(cthd) {
        let arrNhom = [], arrID = [];
        for (let i = 0; i < cthd.length; i++) {
            let itfor = cthd[i];
            if (!arrID.includes(itfor.ID_NhomHangHoa)) {
                arrID.push(itfor.ID_NhomHangHoa);
                let arrHH = cthd.filter(x => x.ID_NhomHangHoa === itfor.ID_NhomHangHoa);
                // assign again STT
                arrHH.map(function (x, i) {
                    x["SoThuTu"] = i + 1;
                })

                let sum = 0, sum_truocVAT = 0, sum_truocCK = 0;
                let sumSL = 0, sumCK = 0, sumVAT = 0;
                for (let k = 0; k < arrHH.length; k++) {
                    let for2 = arrHH[k];
                    sumSL += formatNumberToFloat(for2.SoLuong);
                    sumCK += formatNumberToFloat(for2.TongChietKhau);
                    sumVAT += formatNumberToFloat(for2.TienThue) * formatNumberToFloat(for2.SoLuong);
                    sum += formatNumberToFloat(for2.ThanhToan);
                    sum_truocVAT += formatNumberToFloat(for2.ThanhTien);
                    sum_truocCK += formatNumberToFloat(for2.ThanhTienTruocCK);
                }

                arrNhom.push({
                    SoThuTuNhom: arrNhom.length + 1,
                    SoThuTuNhom_LaMa: ConvertNumber_toRoman(arrNhom.length + 1),
                    ID_NhomHangHoa: itfor.ID_NhomHangHoa,
                    TenNhomHangHoa: itfor.TenNhomHangHoa,
                    TongTienTheoNhom: sum,
                    TongSLTheoNhom: sumSL,
                    TongThueTheoNhom: sumVAT,
                    TongCKTheoNhom: sumCK,
                    TongTienTheoNhom_TruocVAT: sum_truocVAT,
                    TongTienTheoNhom_TruocCK: sum_truocCK,
                    TongTienTheoNhom_TruocCK_SauVAT: sum_truocCK + sumVAT,
                    HangHoas: arrHH
                })
            }
        }
        return arrNhom;
    }

    function Replace_CTHD(newRow, forCTHD) {
        newRow = newRow.replaceAll('{STT}', forCTHD.SoThuTu);
        newRow = newRow.replaceAll("{MaHangHoa}", forCTHD.MaHangHoa);
        newRow = newRow.replaceAll("{TenHangHoa}", forCTHD.TenHangHoa);
        newRow = newRow.replaceAll("{TenHangHoaThayThe}", forCTHD.TenHangHoaThayThe);
        newRow = newRow.replaceAll("{DonGiaBaoHiem}", formatNumber(forCTHD.DonGiaBaoHiem));
        newRow = newRow.replaceAll("{DonViTinh}", forCTHD.TenDonViTinh);
        newRow = newRow.replaceAll("{DonGia}", formatNumber(forCTHD.DonGia));
        newRow = newRow.replaceAll("{GiaBan}", formatNumber(forCTHD.GiaBan));
        newRow = newRow.replaceAll("{GiaBanSauVAT}", formatNumber(forCTHD.GiaBanSauVAT));
        newRow = newRow.replaceAll("{DonGiaSauVAT}", formatNumber(forCTHD.DonGiaSauVAT));
        newRow = newRow.replaceAll("{GiamGia}", formatNumber(forCTHD.TienChietKhau));
        newRow = newRow.replaceAll("{SoLuong}", formatNumber(forCTHD.SoLuong));
        newRow = newRow.replaceAll("{ThanhTien}", formatNumber(forCTHD.ThanhTien));
        newRow = newRow.replaceAll("{ThanhTienTruocCK}", formatNumber(forCTHD.ThanhTienTruocCK));
        newRow = newRow.replaceAll("{TienThue}", formatNumber(forCTHD.TienThue));
        newRow = newRow.replaceAll("{HH_ThueTong}", formatNumber(forCTHD.HH_ThueTong));
        newRow = newRow.replaceAll("{ThanhToan}", formatNumber(forCTHD.ThanhToan));
        newRow = newRow.replaceAll("{MaLoHang}", forCTHD.MaLoHang);
        newRow = newRow.replaceAll("{GhiChu}", forCTHD.GhiChu);
        newRow = newRow.replaceAll("{ThuocTinh_GiaTri}", forCTHD.ThuocTinh_GiaTri);
        newRow = newRow.replaceAll("{PTChietKhauHH}", formatNumber(forCTHD.PTChietKhau));
        newRow = newRow.replaceAll("{PTThue}", formatNumber(forCTHD.PTThue));
        newRow = newRow.replaceAll("{GhiChuHH}", forCTHD.GhiChuHH);
        newRow = newRow.replaceAll("{BH_ThanhTien}", formatNumber(forCTHD.BH_ThanhTien));
        newRow = newRow.replaceAll("{PTChiPhi}", formatNumber(forCTHD.PTChiPhi));
        newRow = newRow.replaceAll("{TienChiPhi}", formatNumber(forCTHD.TienChiPhi));
        newRow = newRow.replaceAll("{TongChietKhau}", formatNumber(forCTHD.TongChietKhau));
        newRow = newRow.replaceAll("{TonKho}", formatNumber(forCTHD.TonKho));
        newRow = newRow.replaceAll("{TonLuyKe}", formatNumber(forCTHD.TonLuyKe));
        newRow = newRow.replaceAll("{NgaySanXuat}", forCTHD.NgaySanXuat);
        newRow = newRow.replaceAll("{NgayHetHan}", forCTHD.NgayHetHan);
        newRow = newRow.replaceAll("{BaoHanh}", forCTHD.BaoHanh);// 6 tháng, 1 năm...
        newRow = newRow.replaceAll("{TenNhomHangHoa}", forCTHD.TenNhomHangHoa);
        newRow = newRow.replaceAll("{GhiChu_NVThucHien}", forCTHD.GhiChu_NVThucHienPrint);
        newRow = newRow.replaceAll("{GhiChu_NVTuVan}", forCTHD.GhiChu_NVTuVanPrint);

        // sudung dv
        newRow = newRow.replaceAll("{SLDVDaSuDung}", formatNumber(forCTHD.SoLuongDVDaSuDung));
        newRow = newRow.replaceAll("{SLDVConLai}", formatNumber(forCTHD.SoLuongDVConLai));
        newRow = newRow.replaceAll("{SoLuongMua}", formatNumber(forCTHD.SoLuongMua));
        //newRow = newRow.replaceAll("{SoPhutThucHien}", DichVuTheoGio==1? ConvertMinutes_ToHourMinutes(ThoiGianThucHien): ThoiGianThucHien);
        newRow = newRow.replaceAll("{ThoiGianBatDau}", forCTHD.TimeStart);
        newRow = newRow.replaceAll("{QuaThoiGian}", forCTHD.QuaThoiGian);
        newRow = newRow.replaceAll("{TenViTri}", forCTHD.TenViTri);

        // chi tiet chuyen hang
        newRow = newRow.replaceAll("{SoLuongChuyen}", formatNumber(forCTHD.SoLuongChuyen));
        newRow = newRow.replaceAll("{SoLuongNhan}", formatNumber(forCTHD.SoLuongNhan));
        newRow = newRow.replaceAll("{GiaChuyen}", formatNumber(forCTHD.GiaChuyen));
        return newRow;
    }

    function Replace_TheoNhom(content, item) {
        content = content.replaceAll("{TenNhomHangHoa}", item.TenNhomHangHoa);
        content = content.replace("{SoThuTuNhom}", item.SoThuTuNhom);
        content = content.replace("{SoThuTuNhom_LaMa}", item.SoThuTuNhom_LaMa);

        content = content.replace("{TongTienTheoNhom}", formatNumber(item.TongTienTheoNhom, 0));
        content = content.replace("{TongTienTheoNhom_TruocVAT}", formatNumber(item.TongTienTheoNhom_TruocVAT, 0));
        content = content.replace("{TongTienTheoNhom_TruocCK}", formatNumber(item.TongTienTheoNhom_TruocCK, 0));
        content = content.replace("{TongTienTheoNhom_TruocCK_SauVAT}", formatNumber(item.TongTienTheoNhom_TruocCK_SauVAT, 0));

        content = content.replace("{TongSLTheoNhom}", formatNumber(item.TongSLTheoNhom));
        content = content.replace("{TongThueTheoNhom}", formatNumber(item.TongThueTheoNhom, 0));
        content = content.replace("{TongCKTheoNhom}", formatNumber(item.TongCKTheoNhom));
        content = content.replace("{TheoNhomHang}", "");
        return content;
    }
    function Caculator_TheoHangHoaDV(cthd) {
        let sumSL = 0, sumThue = 0, sumCK = 0;
        let sumThanhToan = 0, sumThanhTien = 0;
        let sumThanhTien_truocCK = 0;

        for (let k = 0; k < cthd.length; k++) {
            let itFor = cthd[k];
            let soluong = formatNumberToFloat(itFor.SoLuong);
            sumSL += soluong;
            sumThanhToan += formatNumberToFloat(itFor.ThanhToan);
            sumThanhTien += formatNumberToFloat(itFor.ThanhTien);
            sumThanhTien_truocCK += soluong * formatNumberToFloat(itFor.DonGia);
            sumThue += soluong * formatNumberToFloat(itFor.TienThue);
            sumCK += soluong * formatNumberToFloat(itFor.TienChietKhau);
        }
        return {
            SumSoLuong: sumSL,
            SumThue: sumThue,
            SumChietKhau: sumCK,
            SumThanhToan: sumThanhToan,
            SumThanhTien: sumThanhTien,
            SumThanhTien_truocCK: sumThanhTien_truocCK,
            SumThanhTien_TruoCK_sauVAT: sumThanhTien_truocCK + sumThue,
        }
    }
    function Replace_TheoDichVu(content, cthd) {
        let obj = Caculator_TheoHangHoaDV(cthd);
        content = content.replaceAll("{TheoDichVu}", '');
        content = content.replaceAll("{TongSL_DichVu}", obj.SumSoLuong);
        content = content.replace("{TongThue_DichVu}", formatNumber(obj.SumThue, 0));
        content = content.replace("{TongCK_DichVu}", formatNumber(obj.SumChietKhau, 0));
        content = content.replace("{TongTienDichVu}", formatNumber(obj.SumThanhToan, 0));
        content = content.replace("{TongTienDichVu_TruocVAT}", formatNumber(obj.SumThanhTien, 0));
        content = content.replace("{TongTienDichVu_TruocCK}", formatNumber(obj.SumThanhTien_truocCK, 0));
        content = content.replace("{TongTienDichVu_TruocCK_SauVAT}", formatNumber(obj.SumThanhTien_TruoCK_sauVAT, 0));
        return content;
    }

    function Replace_TheoHangHoa(content, cthd) {
        let obj = Caculator_TheoHangHoaDV(cthd);
        content = content.replaceAll("{TheoHangHoa}", '');
        content = content.replaceAll("{TongSL_PhuTung}", obj.SumSoLuong);
        content = content.replace("{TongThue_PhuTung}", formatNumber(obj.SumThue, 0));
        content = content.replace("{TongCK_PhuTung}", formatNumber(obj.SumChietKhau, 0));
        content = content.replace("{TongTienPhuTung}", formatNumber(obj.SumThanhToan, 0));
        content = content.replace("{TongTienPhuTung_TruocVAT}", formatNumber(obj.SumThanhTien, 0));
        content = content.replace("{TongTienPhuTung_TruocCK}", formatNumber(obj.SumThanhTien_truocCK, 0));
        content = content.replace("{TongTienPhuTung_TruocCK_SauVAT}", formatNumber(obj.SumThanhTien_TruoCK_sauVAT, 0));
        return content;
    }

    function Replace_ThongTinChung(content, hd) {
        let cty = self.CongTy()[0];
        content = content.replace("{TenCuaHang}", cty.TenCongTy);
        content = content.replace("{DiaChiCuaHang}", cty.DiaChi);
        content = content.replace("{DienThoaiCuaHang}", cty.SoDienThoai);
        content = content.replace("{Logo}", '<img style="width:100%" src="' + Open24FileManager.hostUrl + cty.DiaChiNganHang + '" />');

        content = content.replace("{TenChiNhanh}", hd.TenChiNhanh);
        content = content.replace("{DienThoaiChiNhanh}", hd.DienThoaiChiNhanh);
        content = content.replace("{DiaChiChiNhanh}", hd.DiaChiChiNhanh);
        return content;
    }

    function Replace_ThongTinKhachHang(content, hd) {
        content = content.replace("{MaKhachHang}", hd.MaDoiTuong);
        content = content.replace("{TenKhachHang}", hd.TenDoiTuong);
        content = content.replace("{DiaChi}", hd.DiaChiKhachHang);
        content = content.replace("{DienThoai}", hd.DienThoai);
        content = content.replace("{TongDiemKhachHang}", hd.TongTichDiem);
        return content;
    }
    function Replace_ThongTinChuXe(content, hd) {
        content = content.replace("{ChuXe}", hd.ChuXe);
        content = content.replace("{ChuXe_SDT}", hd.ChuXe_SDT);
        content = content.replace("{ChuXe_DiaChi}", hd.ChuXe_DiaChi);
        content = content.replace("{ChuXe_Email}", hd.ChuXe_Email);
        return content;
    }
    async function Replace_ThongTinPTN(content, hd) {
        let ptn = await GetThongTinPTN(hd.ID_PhieuTiepNhan);
        if (!$.isEmptyObject(ptn)) {
            content = content.replace("{CoVanDichVu}", ptn.CoVanDichVu);
            content = content.replace("{NhanVienTiepNhan}", ptn.NhanVienTiepNhan);
            content = content.replace("{MaPhieuTiepNhan}", ptn.MaPhieuTiepNhan);
            content = content.replace("{NgayVaoXuong}", ptn.NgayVaoXuong);
            content = content.replace("{NgayHoanThanhDuKien}", ptn.NgayXuatXuongDuKien);
            content = content.replace("{SoKmVao}", ptn.SoKmVao);
            content = content.replace("{SoKmRa}", ptn.SoKmRa);
            content = content.replace("{SoKmCu_PTN}", ptn.SoKmCu_PTN);
            content = content.replace("{SoKmCu}", formatNumber(ptn.SoKmCu, 2));
            content = content.replace("{NgayXuatXuong}", ptn.NgayXuatXuong);
            content = content.replace("{CoVan_SDT}", ptn.CoVan_SDT);
            content = content.replace("{LH_Ten}", ptn.TenLienHe);
            content = content.replace("{PTN_GhiChu}", '<span style="white-space:pre-wrap">' + ptn.PTN_GhiChu + '</span>');

            content = Replace_ThongTinXe(content, ptn);
            content = Replace_ThongTinChuXe(content, ptn);
        }

        return content;
    }

    function Replace_ThongTinXe(content, hd) {
        content = content.replace("{BienSo}", hd.BienSo);
        content = content.replace("{TenMauXe}", hd.TenMauXe);
        content = content.replace("{TenLoaiXe}", hd.TenLoaiXe);
        content = content.replace("{TenHangXe}", hd.TenHangXe);
        content = content.replace("{MauSon}", hd.MauSon);
        content = content.replace("{DungTich}", hd.DungTich);
        content = content.replace("{NamSanXuat}", hd.NamSanXuat);
        content = content.replace("{HopSo}", hd.HopSo);
        content = content.replace("{SoMay}", hd.SoMay);
        content = content.replace("{SoKhung}", hd.SoKhung);
        return content;
    }

    function Replace_ThongTinBaoHiem(content, hd) {
        content = content.replace("{TenBaoHiem}", hd.TenBaoHiem);
        content = content.replace("{BH_SDT}", hd.BH_SDT);
        content = content.replace("{BH_Email}", hd.BH_Email);
        content = content.replace("{BH_DiaChi}", hd.BH_DiaChi);
        content = content.replace("{BH_TenLienHe}", hd.BH_TenLienHe);
        content = content.replace("{BH_SDTLienHe}", hd.BH_SDTLienHe);
        return content;
    }

    function Replace_ThongTinHoaDon(content, hd) {
        content = content.replace("{MaHoaDon}", hd.MaHoaDon);
        content = content.replace("{MaHoaDonTraHang}", hd.MaHoaDonGoc);
        content = content.replace("{NgayLapHoaDon}", moment(hd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm'));
        content = content.replace("{NgayTao}", moment(hd.NgayTao).format('DD/MM/YYYY HH:mm') );
        content = content.replace("{NgayBan}", moment(hd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm'));
        content = content.replace("{NgayApDungGoiDV}", hd.NgayApDungGoiDV);
        content = content.replace("{HanSuDungGoiDV}", hd.HanSuDungGoiDV);
        content = content.replace("{NhanVienBanHang}", hd.TenNhanVien);

        content = content.replace("{DienGiai}", hd.DienGiai);
        content = content.replace("{TongTienHang}", formatNumber(hd.TongTienHang, 2));
        content = content.replace("{TongThanhToan}", formatNumber(hd.TongThanhToan, 2));
        content = content.replace("{TongTienHDSauGiamGia}", formatNumber(hd.TongTienHDSauGiamGia));
        content = content.replace("{DaThanhToan}", formatNumber(hd.DaThanhToan));
        content = content.replace("{ChietKhauHoaDon}", formatNumber(hd.TongGiamGia, 2));
        content = content.replace("{PhiTraHang}", formatNumber(hd.TongChiPhiHangTra));

        content = content.replace("{TongTienHoaDonMua}", formatNumber(hd.TongTienHoaDonMua, 2));
        content = content.replace("{TienTraKhach}", formatNumber(hd.PhaiTraKhach, 0));
        content = content.replace("{KhachCanTra}", formatNumber(hd.PhaiThanhToan, 0));
        content = content.replace("{TongTienTraHang}", formatNumber(hd.TongTienTraHang));
        content = content.replace("{TongTienTra}", formatNumber(hd.TongTienTra, 0));
        content = content.replace("{TongCong}", formatNumber(hd.TongThanhToan, 0));
        content = content.replace("{TongSoLuongHang}", formatNumber(hd.TongSoLuongHang, 2));
        content = content.replace("{ChiPhiNhap}", formatNumber(hd.ChiPhiNhap));
        content = content.replace("{NoTruoc}", formatNumber(hd.NoTruoc, 2));
        content = content.replace("{NoSau}", formatNumber(hd.NoSau, 2));
        content = content.replace("{TienThuaTraKhach}", formatNumber(hd.TienThua, 0));
        content = content.replace("{TienKhachThieu}", formatNumber(hd.TienKhachThieu, 0));
        content = content.replace("{DiemGiaoDich}", hd.DiemGiaoDich);
        content = content.replace("{TongTienThue}", formatNumber(hd.TongTienThue, 0));
        content = content.replace("{TongThueKhachHang}", formatNumber(hd.TongThueKhachHang, 0));
        content = content.replace("{TongGiamGiaHang}", formatNumber(hd.TongGiamGiaHang, 2));
        content = content.replace("{TongTienHangChuaChietKhau}", formatNumber(self.TongTienHangChuaCK(), 2));
        content = content.replace("{PTChietKhauHD}", hd.TongChietKhau);
        content = content.replace("{TienBangChu}", DocSo(hd.TongThanhToan));
        content = content.replace("{KH_TienBangChu}", DocSo(hd.PhaiThanhToan - hd.ThuDatHang - hd.TienDatCoc));

        content = content.replace("{PhaiThanhToan_TruCocBG}", formatNumber(hd.PhaiThanhToan - hd.ThuDatHang));
        content = content.replace("{ThuDatHang}", formatNumber(hd.ThuDatHang));
        content = content.replace("{TienPOS}", formatNumber(hd.TienATM));
        content = content.replace("{TienMat}", formatNumber(hd.TienMat, 0));
        content = content.replace("{TienChuyenKhoan}", formatNumber(hd.ChuyenKhoan, 0));
        content = content.replace("{TraLaiTienDatCoc}", formatNumber(hd.TraLaiTienDatCoc));
        content = content.replace("{TTBangTienCoc}", formatNumber(hd.TTBangTienCoc));
        content = content.replace("{TienDoiDiem}", formatNumber(hd.TienDoiDiem));
        content = content.replace("{TienTheGiaTri}", formatNumber(hd.TienTheGiaTri));
        content = content.replace("{TienTheGiaTri_TruocTT}", formatNumber(hd.TienTheGiaTri_TruocTT));

        content = content.replace("{TenNganHangPOS}", hd.TenNganHangPOS);
        content = content.replace("{TenChuThePOS}", hd.TenChuThePOS);
        content = content.replace("{SoTaiKhoanPOS}", hd.SoTaiKhoanPOS);
        content = content.replace("{TenNganHangChuyenKhoan}", hd.TenNganHangChuyenKhoan);
        content = content.replace("{TenChuTheChuyenKhoan}", hd.TenChuTheChuyenKhoan);
        content = content.replace("{SoTaiKhoanChuyenKhoan}", hd.SoTaiKhoanChuyenKhoan);

        content = content.replace("{TongGiamGiaHD_HH}", formatNumber(hd.TongGiamGiaHD_HH));
        content = content.replace("{ChietKhauNVHoaDon}", hd.ChietKhauNVHoaDon);
        content = content.replace("{ChietKhauNVHoaDon_InGtriCK}", hd.ChietKhauNVHoaDon_InGtriCK);

        content = content.replace("{BH_TienThua}", formatNumber(hd.BH_TienThua));
        content = content.replace("{BH_ConThieu}", formatNumber(hd.BH_ConThieu, 0));
        content = content.replace("{HD_TienThua}", hd.HD_TienThua);
        content = content.replace("{HD_ConThieu}", formatNumber(hd.HD_ConThieu, 0));
        content = content.replace("{BH_TienBangChu}", DocSo(hd.PhaiThanhToanBaoHiem));
        content = content.replace("{BaoHiemDaTra}", formatNumber(hd.BaoHiemDaTra, 0));

        content = content.replace("{PhaiThanhToanBaoHiem}", formatNumber(hd.PhaiThanhToanBaoHiem, 0));
        content = content.replace("{SoVuBaoHiem}", hd.SoVuBaoHiem);
        content = content.replace("{KhauTruTheoVu}", formatNumber(hd.KhauTruTheoVu, 0));
        content = content.replace("{PTGiamTruBoiThuong}", hd.PTGiamTruBoiThuong);
        content = content.replace("{GiamTruBoiThuong}", formatNumber(hd.GiamTruBoiThuong, 0));
        content = content.replace("{TongTienThueBaoHiem}", formatNumber(hd.TongTienThueBaoHiem, 0));
        content = content.replace("{PTThueBaoHiem}", hd.PTThueBaoHiem);
        content = content.replace("{BHThanhToanTruocThue}", formatNumber(hd.BHThanhToanTruocThue, 0));
        content = content.replace("{TongTienBHDuyet}", formatNumber(hd.TongTienBHDuyet));
        return content;
    }

    async function ReplaceFull_ThongTinHoaDon(newHD, hdDB) {
        newHD = Replace_ThongTinChung(newHD, hdDB);
        newHD = Replace_ThongTinHoaDon(newHD, hdDB);
        newHD = Replace_ThongTinKhachHang(newHD, hdDB);
        newHD = await Replace_ThongTinPTN(newHD, hdDB);
        return newHD;
    }

    function CheckRowContent_HasChiTiet(content) {
        return content.indexOf('TenHangHoa') > -1 || content.indexOf('{SoLuong}') > -1
            || content.indexOf('{ThanhTien') > -1
            || content.indexOf('NVThucHien') > -1 || content.indexOf('NVTuVan') > -1;
    }

    self.PrintMany = async function () {
        let content = await GetContent_MauInMacDinh('HDBL');
        let contentGoc = content;

        if (content.indexOf('TheoHangHoa_Nhom') > -1 || content.indexOf('TheoDichVu_Nhom') > -1) {
            // tblhanghoa
            let startHH = content.indexOf("{TheoHangHoa_Nhom}");
            let opentblHH = content.indexOf("tbody", startHH) - 1;
            let closeblHH = content.indexOf("tbody", opentblHH + 6);
            let sTblHH = content.substr(opentblHH, closeblHH - opentblHH + 6);
            let sTblHH_goc = sTblHH;

            let hh_headerFrom = content.indexOf("thead", startHH) - 1;
            let hh_headerTo = content.indexOf("thead", hh_headerFrom + 5);
            let hh_sHeader = content.substr(hh_headerFrom, hh_headerTo - hh_headerFrom + 6);
            let hh_sHeaderGoc = hh_sHeader;

            // tennhomhang
            let hh_row1From = sTblHH.indexOf("<tr");
            let hh_row1To = sTblHH.indexOf("/tr>", hh_row1From + 3) + 4;

            // chitiethang
            let hh_row2From = sTblHH.indexOf("<tr", hh_row1To);
            let hh_row2To = sTblHH.indexOf("/tr>", hh_row2From);
            let hh_row2Str = sTblHH.substr(hh_row2From, hh_row2To - hh_row2From + 5);
            let hh_row2Goc = hh_row2Str;

            // tblDichVu
            let startDV = content.indexOf("{TheoDichVu_Nhom}");
            let opentblDV = content.indexOf("tbody", startDV) - 1;
            let closeblDV = content.indexOf("tbody", opentblDV + 6);
            let sTblDV = content.substr(opentblDV, closeblDV - opentblDV + 6);
            let sTblDV_goc = sTblDV;

            let dv_headerFrom = content.indexOf("thead", startDV) - 1;
            let dv_headerTo = content.indexOf("thead", dv_headerFrom + 5);
            let dv_sHeader = content.substr(dv_headerFrom, dv_headerTo - dv_headerFrom + 6);
            let dv_sHeaderGoc = dv_sHeader;

            let dv_row1From = sTblDV.indexOf("<tr");
            let dv_row1To = sTblDV.indexOf("/tr>", hh_row1From + 3) + 4;

            let dv_row2From = sTblDV.indexOf("<tr", dv_row1To);
            let dv_row2To = sTblDV.indexOf("/tr>", dv_row2From) + 5;
            let dv_row2Str = sTblDV.substr(dv_row2From, dv_row2To - dv_row2From);
            let dv_row2Goc = dv_row2Str;

            for (let k = 0; k < arrIDCheck.length; k++) {
                let idHoaDon = arrIDCheck[k];
                let newHD = contentGoc;
                let sTblHHNew = sTblHH;
                let sTblDVNew = sTblDV;

                let hdDB = await GetInforHD_fromDB(idHoaDon);
                let cthd = await GetChiTietHD_fromDB(idHoaDon);

                let cthd_HangHoa = cthd.filter(x => x.LaHangHoa);
                let cthd_DichVu = cthd.filter(x => !x.LaHangHoa);

                let nhomHH = GroupCTHD_byNhomHang(cthd_HangHoa);
                let nhomDV = GroupCTHD_byNhomHang(cthd_DichVu);

                if (startHH > -1) {
                    if (nhomHH.length === 0) {
                        // remove tblHangHoa
                        sTblHHNew = '';
                        newHD = newHD.replace(hh_sHeaderGoc, '');
                    }
                    else {
                        let ctTheoNhomHH = '';
                        for (let i = 0; i < nhomHH.length; i++) {
                            let forOut = nhomHH[i];
                            for (let j = 0; j < forOut.HangHoas.length; j++) {
                                let newRow = hh_row2Goc;
                                let forCTHD = forOut.HangHoas[j];
                                newRow = Replace_CTHD(newRow, forCTHD);
                                ctTheoNhomHH = ctTheoNhomHH.concat(newRow);
                            }
                            let newNhom = sTblHH_goc;
                            newNhom = Replace_TheoNhom(newNhom, forOut);
                            if (i === 0) {
                                sTblHHNew = sTblHHNew.replace(sTblHH_goc, newNhom);
                                sTblHHNew = sTblHHNew.replace(hh_row2Goc, ctTheoNhomHH);
                            }
                            else {
                                newNhom = newNhom.replace(hh_row2Goc, ctTheoNhomHH);
                                sTblHHNew = sTblHHNew.concat(newNhom);
                            }
                        }
                    }
                }

                if (startDV > -1) {
                    if (nhomDV.length === 0) {
                        // remove tblDV
                        sTblDVNew = '';
                        newHD = newHD.replace(dv_sHeaderGoc, '');
                    }
                    else {
                        let ctTheoNhomDV = '';
                        for (let i = 0; i < nhomDV.length; i++) {
                            let forOut = nhomDV[i];
                            for (let j = 0; j < forOut.HangHoas.length; j++) {
                                let newRow = dv_row2Goc;
                                let forCTHD = forOut.HangHoas[j];
                                newRow = Replace_CTHD(newRow, forCTHD);
                                ctTheoNhomDV = ctTheoNhomDV.concat(newRow);
                            }
                            let newNhom = sTblDV_goc;
                            newNhom = Replace_TheoNhom(newNhom, forOut);
                            if (i === 0) {
                                sTblDVNew = sTblDVNew.replace(sTblDV_goc, newNhom);
                                sTblDVNew = sTblDVNew.replace(dv_row2Goc, ctTheoNhomDV);
                            }
                            else {
                                newNhom = newNhom.replace(dv_row2Goc, ctTheoNhomDV);
                                sTblDVNew = sTblDVNew.concat(newNhom);
                            }
                        }
                    }
                }

                newHD = newHD.replace(sTblHH_goc, sTblHHNew);
                newHD = newHD.replace(sTblDV_goc, sTblDVNew);
                newHD = Replace_TheoHangHoa(newHD, cthd);
                newHD = Replace_TheoDichVu(newHD, cthd);
                Caculator_ChiTietHD(cthd);
                newHD = await ReplaceFull_ThongTinHoaDon(newHD, hdDB);

                if (k === 0) {
                    content = newHD;
                }
                else {
                    content = content.concat(newHD);
                }

                if (k < arrIDCheck.length - 1) {
                    content = content.concat('<p style="page-break-before:always;"></p>')
                }
            }

            content = content.replaceAll("{TheoHangHoa_Nhom}", '');
            content = content.replaceAll("{TheoDichVu_Nhom}", '');
        }
        else {
            if (content.indexOf('TheoNhomHang') > -1) {
                let open = content.lastIndexOf("tbody", content.indexOf("{TenNhomHangHoa}")) - 1;
                let close = content.indexOf("tbody", content.indexOf("{TenNhomHangHoa")) + 6;
                let temptable = content.substr(open, close - open);
                let temptableGoc = temptable;

                let row1From = temptable.indexOf("<tr");
                let row1To = temptable.indexOf("/tr>") - 3;
                let row1Str = temptable.substr(row1From, row1To);
                let row1Goc = row1Str;

                let row2From = temptable.indexOf("<tr", temptable.indexOf("<tr") + 1);
                let row2To = temptable.indexOf("/tr>", row2From + 5) + 5;
                let row2Str = temptable.substr(row2From, row2To - row2From);
                let row2Goc = row2Str;

                for (let k = 0; k < arrIDCheck.length; k++) {
                    let idHoaDon = arrIDCheck[k];
                    let newHD = contentGoc;
                    let tblNhomNew = temptableGoc;

                    let hdDB = await GetInforHD_fromDB(idHoaDon);
                    let cthd = await GetChiTietHD_fromDB(idHoaDon);
                    let nhomCTHD = GroupCTHD_byNhomHang(cthd);

                    for (let i = 0; i < nhomCTHD.length; i++) {
                        let forOut = nhomCTHD[i];
                        let ctTheoNhom = '';
                        for (let j = 0; j < forOut.HangHoas.length; j++) {
                            let newRow = row2Goc;
                            let forCTHD = forOut.HangHoas[j];
                            newRow = Replace_CTHD(newRow, forCTHD);
                            ctTheoNhom = ctTheoNhom.concat(newRow);
                        }

                        let sNhomReplace = Replace_TheoNhom(temptableGoc, forOut);
                        if (i === 0) {
                            tblNhomNew = tblNhomNew.replace(temptableGoc, sNhomReplace);
                            tblNhomNew = tblNhomNew.replace(row2Goc, ctTheoNhom);
                        }
                        else {
                            sNhomReplace = sNhomReplace.replace(row2Goc, ctTheoNhom);
                            tblNhomNew = tblNhomNew.concat(sNhomReplace);
                        }
                    }

                    newHD = newHD.replace(temptableGoc, tblNhomNew);
                    Caculator_ChiTietHD(cthd);
                    newHD = await ReplaceFull_ThongTinHoaDon(newHD, hdDB);

                    if (k === 0) {
                        content = newHD;
                    }
                    else {
                        content = content.concat(newHD);
                    }

                    if (k < arrIDCheck.length - 1) {
                        content = content.concat('<p style="page-break-before:always;"></p>')
                    }
                }

                content = content.replaceAll("{TheoNhomHang}", '');
            }
            else {
                if (content.indexOf('TheoHangHoa') > -1 || content.indexOf('TheoDichVu') > -1) {
                    let open = content.lastIndexOf("tbody", content.indexOf("{TenHangHoa")) - 1;
                    let close = content.indexOf("tbody", content.indexOf("{TenHangHoa")) + 6;
                    let temptable = content.substr(open, close - open);
                    let tblGoc = temptable;
                    let indexHH = content.indexOf("TheoHangHoa");
                    let indexDV = content.indexOf("TheoDichVu");

                    if (indexHH == -1 || indexDV == -1) {
                        // chỉ có hanghoa, hoặc dịch vụ
                        let row1From = temptable.indexOf("<tr");
                        let row1To = temptable.indexOf("/tr>") - 3;
                        let row1Str = temptable.substr(row1From, row1To);
                        let row1Goc = row1Str;

                        // dong2: cthd
                        let row2From = temptable.indexOf("<tr", row1From + 1);
                        let row2To = temptable.indexOf("/tr>", row2From) + 5;
                        let row2Str = '';

                        let sChiTietHD = '';
                        if (CheckRowContent_HasChiTiet(row1Str)) {
                            sChiTietHD = row1Str;
                        }

                        if (row2To > -1) {
                            row2Str = temptable.substr(row2From, row2To - row2From);
                            if (CheckRowContent_HasChiTiet(row2Str)) {
                                sChiTietHD = sChiTietHD.concat(row2Str);
                            }
                        }

                        let row3To = temptable.indexOf("/tr>", row2To) + 5;

                        if (row3To > -1) {
                            row3Str = temptable.substr(row2To, row3To - row2To);
                            if (CheckRowContent_HasChiTiet(row3Str)) {
                                sChiTietHD = sChiTietHD.concat(row3Str);
                            }
                        }

                        for (let k = 0; k < arrIDCheck.length; k++) {
                            let idHoaDon = arrIDCheck[k];
                            let newHD = contentGoc;

                            let hdDB = await GetInforHD_fromDB(idHoaDon);
                            let cthd = await GetChiTietHD_fromDB(idHoaDon);

                            let cthd_HangHoa = cthd.filter(x => x.LaHangHoa);
                            let cthd_DichVu = cthd.filter(x => !x.LaHangHoa);
                            if (indexHH > -1) {
                                let ctHangHoa = '';
                                for (let i = 0; i < cthd_HangHoa.length; i++) {
                                    let newRow = sChiTietHD;
                                    let forCTHD = cthd_HangHoa[i];
                                    forCTHD = AssignNVThucHien_toCTHD(forCTHD);
                                    newRow = Replace_CTHD(newRow, forCTHD);
                                    ctHangHoa = ctHangHoa.concat(newRow);
                                }
                                newHD = newHD.replace(sChiTietHD, ctHangHoa);
                            }
                            if (indexDV > -1) {
                                let ctDichVu = '';
                                for (let i = 0; i < cthd_DichVu.length; i++) {
                                    let newRow = sChiTietHD;
                                    let forCTHD = cthd_DichVu[i];
                                    forCTHD = AssignNVThucHien_toCTHD(forCTHD);
                                    newRow = Replace_CTHD(newRow, forCTHD);
                                    ctDichVu = ctDichVu.concat(newRow);
                                }
                                newHD = newHD.replace(sChiTietHD, ctDichVu);
                            }

                            newHD = Replace_TheoHangHoa(newHD, cthd_HangHoa);
                            newHD = Replace_TheoDichVu(newHD, cthd_DichVu);
                            Caculator_ChiTietHD(cthd);
                            newHD = await ReplaceFull_ThongTinHoaDon(newHD, hdDB);

                            if (k === 0) {
                                content = newHD;
                            }
                            else {
                                content = content.concat(newHD);
                            }

                            if (k < arrIDCheck.length - 1) {
                                content = content.concat('<p style="page-break-before:always;"></p>')
                            }

                        }
                    }
                    else {
                        // chungbang
                        // dong1: (TheoHangHoa/TheoDichVu)
                        let row1From = temptable.indexOf("<tr");
                        let row1To = temptable.indexOf("/tr>") - 3;
                        let row1Str = temptable.substr(row1From, row1To);
                        let row1Goc = row1Str;

                        // dong2: cthd
                        let row2From = temptable.indexOf("<tr", temptable.indexOf("<tr") + 1);
                        let row2To = temptable.indexOf("<tr", row2From + 1);
                        let row2Str = temptable.substr(row2From, row2To - row2From);

                        // dong3: tongcong /hoac {TheoHangHoa/DV}
                        let row3To = temptable.indexOf("/tr>", row2To + 5) + 5;
                        let row3Str = temptable.substr(row2To, row3To - row2To);

                        // dong4: cthd hoac (TheoHangHoa/TheoDichVu)
                        let row4To = temptable.indexOf("/tr>", row3To + 5) + 5;
                        let row4Str = temptable.substr(row3To, row4To - row3To);

                        // dong5: cthd
                        let row5To = temptable.indexOf("/tr>", row4To + 5) + 5;
                        let row5Str = '';
                        if (row5To > -1) {
                            row5Str = temptable.substr(row4To, row5To - row4To);
                        }

                        // tongcong
                        let row6To = temptable.indexOf("/tr>", row5To + 5) + 5;
                        let row6Str = '';
                        if (row6To > -1) {
                            row6Str = temptable.substr(row5To, row6To - row5To);
                        }

                        let hh_tblGoc = '';
                        let dv_tblGoc = '';
                        let rowHangHoa = '';
                        let rowDichVu = '';

                        if (indexHH < indexDV) {
                            // hanghoa truoc, dvsau
                            rowHangHoa = row2Str;
                            if (row3Str.indexOf('TongTienPhuTung') > -1) {
                                hh_tblGoc = row1Str.concat(row2Str, row3Str);
                                dv_tblGoc = row4Str.concat(row5Str, row6Str);
                                rowDichVu = row5Str;
                            }
                            else {
                                hh_tblGoc = row1Str.concat(row2Str);
                                dv_tblGoc = row3Str.concat(row4Str, row5Str);
                                rowDichVu = row4Str;
                            }
                        }
                        else {
                            rowDichVu = row2Str;
                            if (row3Str.indexOf('TongTienDichVu') > -1) {
                                dv_tblGoc = row1Str.concat(row2Str, row3Str);
                                hh_tblGoc = row4Str.concat(row5Str, row6Str);
                                rowHangHoa = row5Str;
                            }
                            else {
                                dv_tblGoc = row1Str.concat(row2Str);
                                hh_tblGoc = row3Str.concat(row4Str, row5Str);
                                rowHangHoa = row4Str;
                            }
                        }

                        for (let k = 0; k < arrIDCheck.length; k++) {
                            let idHoaDon = arrIDCheck[k];
                            let newHD = contentGoc;
                            let sTblHHNew = hh_tblGoc;
                            let sTblDVNew = dv_tblGoc;

                            let hdDB = await GetInforHD_fromDB(idHoaDon);
                            let cthd = await GetChiTietHD_fromDB(idHoaDon);

                            let cthd_HangHoa = cthd.filter(x => x.LaHangHoa);
                            let cthd_DichVu = cthd.filter(x => !x.LaHangHoa);

                            let ctHangHoa = '';
                            for (let i = 0; i < cthd_HangHoa.length; i++) {
                                let newRow = rowHangHoa;
                                let forCTHD = cthd_HangHoa[i];
                                newRow = Replace_CTHD(newRow, forCTHD);
                                ctHangHoa = ctHangHoa.concat(newRow);
                            }
                            let ctDichVu = '';
                            for (let i = 0; i < cthd_DichVu.length; i++) {
                                let newRow = rowDichVu;
                                let forCTHD = cthd_DichVu[i];
                                newRow = Replace_CTHD(newRow, forCTHD);
                                ctDichVu = ctDichVu.concat(newRow);
                            }
                            sTblHHNew = sTblHHNew.replace(rowHangHoa, ctHangHoa);
                            sTblDVNew = sTblDVNew.replace(rowDichVu, ctDichVu);

                            newHD = newHD.replace(hh_tblGoc, sTblHHNew);
                            newHD = newHD.replace(dv_tblGoc, sTblDVNew);
                            newHD = Replace_TheoHangHoa(newHD, cthd_HangHoa);
                            newHD = Replace_TheoDichVu(newHD, cthd_DichVu);
                            Caculator_ChiTietHD(cthd);
                            newHD = await ReplaceFull_ThongTinHoaDon(newHD, hdDB);

                            if (k === 0) {
                                content = newHD;
                            }
                            else {
                                content = content.concat(newHD);
                            }

                            if (k < arrIDCheck.length - 1) {
                                content = content.concat('<p style="page-break-before:always;"></p>')
                            }
                        }

                    }

                }
                else {
                    if (content.indexOf("{Combo}") > -1) {
                        // todo: mẫu combo
                    }
                    else {
                        if (content.indexOf('TenHangHoa') > -1 || content.indexOf('MaHangHoa') > -1) {
                            // mẫu cơ bản nhất
                            let open = content.lastIndexOf("tbody", content.indexOf("{TenHangHoa")) - 1;
                            let close = content.indexOf("tbody", content.indexOf("{TenHangHoa")) + 6;
                            let temptable = content.substr(open, close - open);
                            let tblGoc = temptable;

                            let row1From = temptable.indexOf("<tr");
                            let row1To = temptable.indexOf("/tr>") - 3;
                            let row1Str = temptable.substr(row1From, row1To);
                            let row1Goc = row1Str;

                            // dong2: cthd
                            let row2From = temptable.indexOf("<tr", row1From + 1);
                            let row2To = temptable.indexOf("/tr>", row2From) + 5;
                            let row2Str = '';

                            let sChiTietHD = '';
                            if (CheckRowContent_HasChiTiet(row1Str)) {
                                sChiTietHD = row1Str;
                            }

                            if (row2To > -1) {
                                row2Str = temptable.substr(row2From, row2To - row2From);
                                if (CheckRowContent_HasChiTiet(row2Str)) {
                                    sChiTietHD = sChiTietHD.concat(row2Str);
                                }
                            }

                            let row3To = temptable.indexOf("/tr>", row2To) + 5;

                            if (row3To > -1) {
                                row3Str = temptable.substr(row2To, row3To - row2To);
                                if (CheckRowContent_HasChiTiet(row3Str)) {
                                    sChiTietHD = sChiTietHD.concat(row3Str);
                                }
                            }

                            for (let k = 0; k < arrIDCheck.length; k++) {
                                let idHoaDon = arrIDCheck[k];
                                let newHD = contentGoc;

                                let hdDB = await GetInforHD_fromDB(idHoaDon);
                                let cthd = await GetChiTietHD_fromDB(idHoaDon);

                                let ctHangHoa = '';
                                for (let i = 0; i < cthd.length; i++) {
                                    let newRow = sChiTietHD;
                                    let forCTHD = cthd[i];
                                    forCTHD = AssignNVThucHien_toCTHD(forCTHD);
                                    newRow = Replace_CTHD(newRow, forCTHD);
                                    ctHangHoa = ctHangHoa.concat(newRow);
                                }
                                newHD = newHD.replace(sChiTietHD, ctHangHoa);

                                Caculator_ChiTietHD(cthd);
                                newHD = await ReplaceFull_ThongTinHoaDon(newHD, hdDB);

                                if (k === 0) {
                                    content = newHD;
                                }
                                else {
                                    content = content.concat(newHD);
                                }

                                if (k < arrIDCheck.length - 1) {
                                    content = content.concat('<p style="page-break-before:always;"></p>')
                                }
                            }
                        }
                    }
                }
            }
        }

        PrintExtraReport(content);
    }

    self.PrintMany1 = async function () {
        let arHD = [];
        let obj = await GetListHDbyIDs();

        if (!$.isEmptyObject(obj)) {
            let lstHD = obj.lstHD;
            let lstCTHD = obj.lstCTHD;
            for (let i = 0; i < lstHD.length; i++) {
                let forHD = lstHD[i];
                let arrCT = $.grep(lstCTHD, function (x) {
                    return x.ID_HoaDon === forHD.ID;
                });

                if (arrCT.length > 0) {
                    let objHD = await GetInforHDPrint(forHD.ID, false, arrCT);
                    let cthdPrint = await GetCTHDPrint_Format(forHD.ID);                  
                    objHD.BH_HoaDon_ChiTiet = cthdPrint;
                    objHD.CTHoaDonPrintMH = [];
                    arHD.push(objHD);
                }
            }

            $.ajax({
                url: '/api/DanhMuc/ThietLapApi/GetContentFIlePrintTypeChungTu?maChungTu=HDBL&idDonVi='
                    + VHeader.IdDonVi + '&printMultiple=true',
                type: 'GET',
                dataType: 'json',
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                success: function (result) {
                    let data = result;
                    data = data.concat('<script src="/Scripts/knockout-3.4.2.js"></script>');                 
                    data = data.concat(' <script src="/Content/Framework/Moment/moment.min.js"></script>');
                    data = data.concat(`<script> function formatNumber(number, decimalDot = 2) {
                                            if (number === undefined || number === null) {
                                                return 0;
                                            }
                                            else {
                                                number = formatNumberToFloat(number);
                                                number = Math.round(number * Math.pow(10, decimalDot)) / Math.pow(10, decimalDot);
                                                if (number !== null) {
                                                    let lastone = number.toString().split('').pop();
                                                    if (lastone !== '.') {
                                                        number = parseFloat(number);
                                                    }
                                                }
                                                if (isNaN(number)) {
                                                    number = 0;
                                                }
                                                let xxxx=number.toString().replace(/\\B(?=(\\d{3})+(?!\\d))/g, ',');
                                                return xxxx;
                                            }
                                        }
                                    function formatNumberToFloat(objVal) {
                                            let value = parseFloat(objVal.toString().replace(/,/g, ''));
                                            if (isNaN(value)) {
                                                return 0;
                                            }
                                            else {
                                             
                                                return value;
                                            }
                                       
                                    } </script>`);
                    data = data.concat("<script > var item1=" + JSON.stringify(arHD) + "; </script>");
                    data = data.concat('<script> var dataMauIn = function () {' +
                        'var self = this;' +
                        'self.ListHoaDon_ChiTietHoaDonPrint = ko.observableArray(item1);' +
                        'self.Count_ListHoaDons = ko.computed(function () { ' +
                        'return self.ListHoaDon_ChiTietHoaDonPrint().length;' +
                        '})' +
                        '};' +
                        'ko.applyBindings(new dataMauIn()) </script>');
                    PrintExtraReport_Multiple(data);
                }
            });
        }
    }

    self.PrintMerger = async function () {
        var idCus = null, idBaoHiem = null;
        if (arrIDCheck.length > 0) {
            // find hd by id --> get customer
            let hd = $.grep(self.HoaDons(), function (x) {
                return $.inArray(x.ID, arrIDCheck) > -1;
            });
            if (hd.length > 0) {
                idCus = hd[0].ID_DoiTuong;
                idBaoHiem = hd[0].ID_BaoHiem;
            }
        }

        var cus = await GetInforCus(idCus);
        var baohiem = await GetInforCus(idBaoHiem);

        ajaxHelper(BH_HoaDonUri + 'GetListHDbyIDs?lstID=' + arrIDCheck, 'POST', arrIDCheck).done(function (x) {
            if (x.res) {
                // khac PTN --> canh bao
                if ($.unique($.map(x.lstHD, function (x) { return x.ID_PhieuTiepNhan })).length > 1) {
                    commonStatisJs.ShowMessageDanger('Không in gộp hóa đơn nếu khác phiếu tiếp nhận');
                    return;
                }

                let lstHD = x.lstHD;
                let lstCTHD = x.lstCTHD;

                let multipleHD = lstHD.length > 1;
                if (lstHD.length > 0) {
                    let itFirstHD = lstHD[0];

                    let tn = {};
                    $.getJSON('/api/DanhMuc/GaraAPI/' + 'PhieuTiepNhan_GetThongTinChiTiet?id=' + itFirstHD.ID_PhieuTiepNhan).done(function (x) {
                        if (x.res && x.dataSoure.length > 0) {
                            tn = x.dataSoure[0];
                        }

                        let arrID_Khach = [], arrID_NhanVien = [], arrID_BaoHiem = [];
                        let maHD = '', maBG = '', nguoitaoHD = '', ngaylapHD = '', diengiai = '',
                            maKhachs = '', tenKhachs = '', tenNVienBans = '', maBaoHiems = '', tenBaoHiems = '';
                        let diemgiaodich = 0, tongthueKH = 0;

                        for (let i = 0; i < lstHD.length; i++) {
                            let forHD = lstHD[i];

                            maHD += forHD.MaHoaDon + ', ';
                            maBG += forHD.MaHoaDonGoc + ', ';
                            diengiai += forHD.DienGiai + ' <br /> ';
                            ngaylapHD += moment(forHD.NgayLapHoaDon).format('DD/MM/YYYY HH:mm:ss') + ', ';
                            nguoitaoHD = forHD.NguoiTaoHD;

                            if ($.inArray(forHD.ID_DoiTuong, arrID_Khach) === -1) {
                                arrID_Khach.push(forHD.ID_DoiTuong);
                                maKhachs += forHD.MaDoiTuong + ', ';
                                tenKhachs += forHD.TenDoiTuong + ', ';
                            }

                            if ($.inArray(forHD.ID_BaoHiem, arrID_BaoHiem) === -1) {
                                arrID_BaoHiem.push(forHD.ID_BaoHiem);
                                maBaoHiems += forHD.MaBaoHiem + ', ';
                                tenBaoHiems += forHD.TenBaoHiem + ', ';
                            }

                            if ($.inArray(forHD.ID_NhanVien, arrID_NhanVien) === -1) {
                                arrID_NhanVien.push(forHD.ID_NhanVien);
                                tenNVienBans += forHD.TenNhanVien + ', ';
                            }

                            diemgiaodich += forHD.DiemGiaoDich;
                            tongthueKH += forHD.TongThueKhachHang;
                        }

                        let objPrint = {
                            MaHoaDon: Remove_LastComma(maHD),
                            MaHoaDonTraHang: Remove_LastComma(maBG),
                            NgayLapHoaDon: Remove_LastComma(ngaylapHD),
                            Ngay: moment(itFirstHD.NgayLapHoaDon).format('DD'),
                            Thang: moment(itFirstHD.NgayLapHoaDon).format('MM'),
                            Nam: moment(itFirstHD.NgayLapHoaDon).format('YYYY'),

                            LoaiHoaDon: itFirstHD.LoaiHoaDon,
                            NguoiTao: nguoitaoHD,
                            DienGiai: diengiai,
                            NhanVienBanHang: tenNVienBans,
                            TenGiaBan: 'Bảng giá chuẩn',

                            ChoThanhToan: false,
                            NgayApDungGoiDV: null,
                            HanSuDungGoiDV: null,
                            DiemGiaoDich: diemgiaodich,
                            TongChiPhi: itFirstHD.SumTongChiPhi,

                            TongTienHang: itFirstHD.SumTongChiPhi,
                            PhaiThanhToan: itFirstHD.SumPhaiThanhToan,
                            TongGiamGiaKM_HD: itFirstHD.SumTongGiamGia,
                            TongGiamGia: itFirstHD.SumTongGiamGia,
                            DaThanhToan: itFirstHD.SumDaThanhToan,
                            KhachDaTra: itFirstHD.SumKhachDaTra,
                            BaoHiemDaTra: itFirstHD.SumBaoHiemDaTra,
                            TongTienThue: itFirstHD.SumTongTienThue,

                            TongTienThueBaoHiem: itFirstHD.SumTongTienThueBaoHiem,
                            PhaiThanhToanBaoHiem: itFirstHD.SumPhaiThanhToanBaoHiem,
                            TongThanhToan: itFirstHD.SumTongThanhToan,
                            KhauTruTheoVu: itFirstHD.SumKhauTruTheoVu,
                            GiamTruBoiThuong: 0,
                            TongTienThueBaoHiem: itFirstHD.SumTongTienThueBaoHiem,
                            BHThanhToanTruocThue: itFirstHD.SumBHThanhToanTruocThue,
                            TongTienBHDuyet: itFirstHD.SumTongTienBHDuyet,
                            TongThueKhachHang: tongthueKH,
                            TongCong: itFirstHD.SumTongThanhToan,

                            PTThueHoaDon: multipleHD ? 0 : itFirstHD.PTThueHoaDon,
                            SoVuBaoHiem: multipleHD ? 0 : itFirstHD.SoVuBaoHiem,
                            PTThueBaoHiem: multipleHD ? 0 : itFirstHD.PTThueBaoHiem,
                            PTGiamTruBoiThuong: multipleHD ? 0 : itFirstHD.PTGiamTruBoiThuong,
                            TongChietKhau: multipleHD ? 0 : itFirstHD.TongChietKhau,

                            CongThucBaoHiem: 0,
                            PTThueKhachHang: 0,
                            GiamTruThanhToanBaoHiem: 0,
                            KhuyeMai_GiamGia: 0,
                            TongTienTra: 0,
                            TongTienMua: 0,
                            TongGiaGocHangTra: 0,
                            TongChiPhiHangTra: 0,
                            HoanTraThuKhac: 0,
                            DaTraKhach: 0,
                            PhaiTraKhach: 0,
                            DiemQuyDoi: 0,
                            TienThua: 0,

                            TienMat: itFirstHD.SumTienMat,
                            TienATM: itFirstHD.SumPOS,
                            TienGui: itFirstHD.SumChuyenKhoan,
                            TienTheGiaTri: itFirstHD.SumThuTuThe,
                            TTBangDiem: itFirstHD.SumTienDoiDiem,
                            MaDoiTuong: Remove_LastComma(maKhachs),
                            TenDoiTuong: Remove_LastComma(tenKhachs),
                            DienThoaiKhachHang: itFirstHD.DienThoai,
                            DiaChiKhachHang: itFirstHD.DiaChiKhachHang,
                            MaPhieuTiepNhan: '',
                            TenBaoHiem: Remove_LastComma(tenBaoHiems),
                            MaBaoHiem: Remove_LastComma(maBaoHiems),

                            NgayVaoXuong: '',
                            NgayXuatXuongDuKien: '',
                            LienHeBaoHiem: '',
                            SoDienThoaiLienHeBaoHiem: '',
                            PTN_GhiChu: '',
                            CoVan_SDT: '',
                            CoVanDichVu: '',
                            NhanVienTiepNhan: '',
                            SoKhung: '',
                            SoMay: '',
                            HopSo: '',
                            DungTich: '',
                            NamSanXuat: '',
                            MauSon: '',
                            TenLoaiXe: '',
                            TenMauXe: '',
                            TenHangXe: '',
                            ChuXe: '',
                            MaSoThue: cus.MaSoThue,
                            TaiKhoanNganHang: '',

                            BH_SDT: '',
                            BH_Email: '',
                            BH_DiaChi: '',
                            BH_TenLienHe: '',
                            BH_SDTLienHe: '',
                            BH_MaSoThue: baohiem.MaSoThue,

                            ChiPhi_GhiChu: '',
                            PTChietKhauHH: 0,
                            TongGiamGiaHang: 0,
                            TongTienHangChuaCK: 0,
                            TongTienKhuyenMai_CT: 0,
                            TongGiamGiaKhuyenMai_CT: 0,
                            SoDuDatCoc: 0,
                            NoTruoc: 0,
                            BH_NoTruoc: 0,
                            BH_NoSau: 0,
                            HD_ConThieu: itFirstHD.SumConNo,
                            TienKhachThieu: itFirstHD.SumPhaiThanhToan - itFirstHD.SumKhachDaTra,
                            BH_ConThieu: itFirstHD.SumPhaiThanhToanBaoHiem - itFirstHD.SumBaoHiemDaTra,
                        }

                        let notruoc = 0, nosau = 0;
                        if (cus) {
                            nosau = cus.NoHienTai;
                            notruoc = nosau - objPrint.TienKhachThieu;
                            notruoc = notruoc < 0 ? 0 : notruoc;
                        }
                        else {
                            nosau = itFirstHD.SumConNo;// khachle
                        }
                        objPrint.NoTruoc = formatNumber(notruoc);
                        objPrint.NoSau = formatNumber(nosau);
                        objPrint.NoSau_BangChu = DocSo(nosau);
                        objPrint.NgayIn = formatDateTime(new Date());

                        let pthuc = '';
                        if (itFirstHD.SumTienMat > 0) {
                            pthuc += 'Tiền mặt, ';
                        }
                        if (itFirstHD.SumPOS > 0) {
                            pthuc += 'POS, ';
                        }
                        if (itFirstHD.SumChuyenKhoan > 0) {
                            pthuc += 'Chuyển khoản, ';
                        }
                        if (itFirstHD.SumThuTuThe > 0) {
                            pthuc += 'Thẻ giá trị, ';
                        }
                        if (itFirstHD.SumTienDoiDiem > 0) {
                            pthuc += 'Điểm, ';
                        }
                        if (itFirstHD.SumTienCoc > 0) {
                            pthuc += 'Tiền cọc, ';
                        }

                        objPrint.PhuongThucTT = Remove_LastComma(pthuc);
                        objPrint.TienBangChu = DocSo(objPrint.TongCong);
                        objPrint.KH_TienBangChu = DocSo(itFirstHD.SumKhachDaTra);
                        objPrint.BH_TienBangChu = DocSo(itFirstHD.SumBaoHiemDaTra);

                        if (tn) {
                            objPrint.MaPhieuTiepNhan = tn.MaPhieuTiepNhan;
                            objPrint.BienSo = tn.BienSo;
                            objPrint.ChuXe = tn.ChuXe;
                            objPrint.ChuXe_DiaChi = tn.ChuXe_DiaChi;
                            objPrint.ChuXe_Email = tn.ChuXe_Email;
                            objPrint.ChuXe_SDT = tn.ChuXe_SDT;
                            objPrint.CoVanDichVu = tn.CoVanDichVu;
                            objPrint.CoVan_SDT = tn.CoVan_SDT;
                            objPrint.DungTich = tn.DungTich;
                            objPrint.PTN_GhiChu = tn.GhiChu;
                            objPrint.HopSo = tn.HopSo;
                            objPrint.NhanVienTiepNhan = tn.NhanVienTiepNhan;
                            objPrint.MauSon = tn.MauSon;
                            objPrint.NamSanXuat = tn.NamSanXuat;
                            objPrint.NgayVaoXuong = moment(tn.NgayVaoXuong).format('DD/MM/YYYY HH:mm');
                            objPrint.NgayXuatXuong = tn.NgayXuatXuong ? moment(tn.NgayXuatXuong).format('DD/MM/YYYY HH:mm') : '';
                            objPrint.NgayXuatXuongDuKien = tn.NgayXuatXuongDuKien ? moment(tn.NgayXuatXuongDuKien).format('DD/MM/YYYY HH:mm') : '';
                            objPrint.SoDienThoaiLienHe = tn.SoDienThoaiLienHe;
                            objPrint.SoKhung = tn.SoKhung;
                            objPrint.SoMay = tn.SoMay;
                            objPrint.SoKmRa = tn.SoKmRa;
                            objPrint.SoKmVao = tn.SoKmVao;
                            objPrint.TenHangXe = tn.TenHangXe;
                            objPrint.TenLoaiXe = tn.TenLoaiXe;
                            objPrint.TenMauXe = tn.TenMauXe;
                            objPrint.TenLienHe = tn.TenLienHe;
                        }

                        let sumSoLuong = 0, sumGiamGiaHang = 0, tongtienhang_truocCK = 0;
                        let cthdPrint = [];
                        for (let j = 0; j < lstCTHD.length; j++) {
                            let ctFor = $.extend({}, true, lstCTHD[j]);
                            ctFor.SoThuTu = j + 1;
                            ctFor.BH_ThanhTien = ctFor.SoLuong * ctFor.DonGiaBaoHiem;
                            ctFor.HH_ThueTong = ctFor.SoLuong * ctFor.TienThue;
                            ctFor.TongChietKhau = ctFor.SoLuong * ctFor.GiamGia;
                            ctFor.ThanhTienTruocCK = ctFor.SoLuong * ctFor.DonGia;
                            cthdPrint.push(ctFor);

                            sumSoLuong += ctFor.SoLuong;
                            sumGiamGiaHang += ctFor.TongChietKhau;
                            tongtienhang_truocCK += ctFor.ThanhTienTruocCK;
                        }
                        objPrint.TongGiamGiaHang = sumGiamGiaHang;
                        objPrint.TongTienHangChuaCK = tongtienhang_truocCK;
                        objPrint.TongSoLuongHang = sumSoLuong;
                        objPrint.TongGiamGiaHD_HH = sumGiamGiaHang + itFirstHD.SumTongGiamGia;

                        let arrHH = lstCTHD.filter(x => x.LaHangHoa);
                        let arrDV = lstCTHD.filter(x => x.LaHangHoa === false);

                        let tongDV = 0, tongDV_truocVAT = 0, tongDV_truocCK = 0;
                        let tongHH = 0, tongHH_truocVAT = 0, tongHH_truocCK = 0;
                        let DV_tongthue = 0, DV_tongCK = 0, DV_tongSL = 0;
                        let HH_tongthue = 0, HH_tongCK = 0, HH_tongSL = 0;
                        for (let k = 0; k < arrHH.length; k++) {
                            let itFor = arrHH[k];
                            let soluong = formatNumberToFloat(itFor.SoLuong);
                            HH_tongSL += soluong;
                            tongHH += formatNumberToFloat(itFor.ThanhToan);
                            tongHH_truocVAT += formatNumberToFloat(itFor.ThanhTien);
                            tongHH_truocCK += soluong * formatNumberToFloat(itFor.DonGia);
                            HH_tongthue += soluong * formatNumberToFloat(itFor.TienThue);
                            HH_tongCK += soluong * formatNumberToFloat(itFor.TienChietKhau);
                        }
                        for (let k = 0; k < arrDV.length; k++) {
                            let itFor = arrDV[k];
                            let soluong = formatNumberToFloat(itFor.SoLuong);
                            DV_tongSL += soluong;
                            tongDV += formatNumberToFloat(itFor.ThanhToan);
                            tongDV_truocVAT += formatNumberToFloat(itFor.ThanhTien);
                            tongDV_truocCK += soluong * formatNumberToFloat(itFor.DonGia);
                            DV_tongthue += soluong * formatNumberToFloat(itFor.TienThue);
                            DV_tongCK += soluong * formatNumberToFloat(itFor.TienChietKhau);
                        }

                        objPrint.TongSL_DichVu = DV_tongSL;
                        objPrint.TongTienDichVu = tongDV;
                        objPrint.TongThue_DichVu = DV_tongthue;
                        objPrint.TongCK_DichVu = DV_tongCK;
                        objPrint.TongTienDichVu_TruocCK = tongDV_truocCK;
                        objPrint.TongTienDichVu_TruocVAT = tongDV_truocVAT;

                        objPrint.TongSL_PhuTung = HH_tongSL;
                        objPrint.TongTienPhuTung = tongHH;
                        objPrint.TongThue_PhuTung = HH_tongthue;
                        objPrint.TongCK_PhuTung = HH_tongCK;
                        objPrint.TongTienPhuTung_TruocCK = tongHH_truocCK;
                        objPrint.TongTienPhuTung_TruocVAT = tongHH_truocVAT;

                        if (self.CongTy().length > 0) {
                            objPrint.LogoCuaHang = Open24FileManager.hostUrl + self.CongTy()[0].DiaChiNganHang;
                            objPrint.TenCuaHang = self.CongTy()[0].TenCongTy;
                            objPrint.DiaChiCuaHang = self.CongTy()[0].DiaChi;
                            objPrint.DienThoaiCuaHang = self.CongTy()[0].SoDienThoai;
                        }

                        $.ajax({
                            url: '/api/DanhMuc/ThietLapApi/GetContentFIlePrintTypeChungTu?maChungTu=HDBL&idDonVi=' + VHeader.IdDonVi,
                            type: 'GET',
                            dataType: 'json',
                            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                            success: function (result) {
                                let data = result;
                                data = data.concat('<script src="/Scripts/knockout-3.4.2.js"></script>');
                                data = data.concat(' <script src="/Content/Framework/Moment/moment.min.js"></script>');
                                data = data.concat("<script > var item1=", JSON.stringify(cthdPrint)
                                    , "; var item2= [] "
                                    , "; var item3=", JSON.stringify(objPrint)
                                    , "; var item4 =", JSON.stringify(self.HangMucSuaChuas())
                                    , "; var item5 =", JSON.stringify(self.VatDungKemTheos())
                                    , "; </script>");
                                data = data.concat(" <script type='text/javascript' src='/Scripts/Thietlap/MauInTeamplate.js'></script>"); // MauInTeamplate.js: used to bind data in knockout
                                PrintExtraReport(data);
                            }
                        });

                    })
                }
            }
        })
    }

    function PrintExtraReport_Multiple(dataContent) {
        var frame1 = $('<iframe />');
        frame1[0].name = "frame1";
        frame1.css({ "position": "absolute", "top": "-100000px" });
        $("body").append(frame1);
        var frameDoc = frame1[0].contentWindow ? frame1[0].contentWindow : frame1[0].contentDocument.document ? frame1[0].contentDocument.document : frame1[0].contentDocument;
        frameDoc.document.open();
        frameDoc.document.write('<html><head>');
        frameDoc.document.write(`<style> tr.mauin-tr-netlien td { border-bottom: 1px solid #ccc; } 
                                tr.mauin-tr-netdut td { border-bottom: 1px dashed #ccc; }
                                table.mauin-table-baoquanh{
                                    border: 1px solid black;
                                }
                                table.mauin-table-baoquanh td,table.mauin-table-baoquanh th{
                                    border: none;
                                } </style>`);
        frameDoc.document.write('</head><body><div style="width:96%">');
        frameDoc.document.write(dataContent);
        frameDoc.document.write('</div></body></html>');
        frameDoc.document.close();
        setTimeout(function () {
            window.frames["frame1"].focus();
            window.frames["frame1"].print();
            frame1.remove();
        }, 500);
    }
    self.Change_LoaiMauIn = function (maChungTu, item) {
        dathangTeamplate = maChungTu;
        loadMauIn();
        if (maChungTu === 'DTH') {
            GetInfor_ofHDDoiTra(item);
        }
    }

    async function GetMauIn_ByMaLoaiChunghTu(maChungTu) {
        const data = await ajaxHelper('/api/DanhMuc/ThietLapApi/GetContentFIlePrintTypeChungTu?maChungTu=' + maChungTu + '&idDonVi=' + id_donvi, 'GET').done()
            .then(function (result) {
                return result;
            });
        return data;
    }

    async function fetchBankAccountData(id) {
        const xx = await ajaxHelper(BH_HoaDonUri + 'GetInforBankAccount_ofHoaDon?idHoaDon=' + id, 'GET').done()
            .then(function (data) {
                if (data.res && data.dataSoure.length > 0) {
                    return data.dataSoure[0];
                }
                return {
                    MaNganHang: '',
                    TenNganHang: '',
                    TenChuThe: '',
                    SoTaiKhoan: '',
                    MaPinNganHang: '',
                    TienThu: 0,
                }
            });
        return xx;
    }

    self.InHoaDon = async function (item) {
        var cthdFormat = await GetCTHDPrint_Format(item.ID);
        self.CTHoaDonPrint(cthdFormat);

        var itemHDFormat = await GetInforHDPrint(item.ID, false);
        self.InforHDprintf(itemHDFormat);

        const dataMauIn = await GetMauIn_ByMaLoaiChunghTu(dathangTeamplate);
        MauInHoaDon_CheckAndBind(dataMauIn);
    }

    self.InHoaDonDoiTra = async function (item) {
        var cthdTraHang = await GetCTHDPrint_Format(item.ID_HoaDon);
        self.CTHoaDonPrint(cthdTraHang);
        var cthdDoiTra = await GetCTHDPrint_Format(item.ID);
        self.CTHoaDonPrintMH(cthdDoiTra);

        // get infor from hdTra
        let tongTienHDTra = 0, phiTraHang = 0, phaiTraKhach = 0;
        for (let i = 0; i < self.HoaDons().length; i++) {
            let itFor = self.HoaDons()[i];
            if (itFor.ID === item.ID_HoaDon) {
                tongTienHDTra = itFor.TongThanhToan;
                phiTraHang = itFor.TongChiPhi;
                break;
            }
        }

        var phaiTT = formatNumberToFloat(item.PhaiThanhToan) - tongTienHDTra;
        if (phaiTT > 0) {
            phaiTraKhach = 0;
        }
        else {
            phaiTraKhach = Math.abs(phaiTT);
            phaiTT = 0;
        }

        let itemHDFormat = await GetInforHDPrint(item.ID, true);
        itemHDFormat.TongTienTraHang = tongTienHDTra;
        itemHDFormat.PhiTraHang = phiTraHang;
        itemHDFormat.PhaiTraKhach = phaiTraKhach;
        itemHDFormat.TongCong = phaiTT;
        itemHDFormat.PhaiThanhToan = phaiTT;
        self.InforHDprintf(itemHDFormat);

        const dataMauIn = await GetMauIn_ByMaLoaiChunghTu('DTH');
        MauInHoaDon_CheckAndBind(dataMauIn);
    }
    self.InPhieuThu = function (item) {
        var temp = phieuThuTeamplate;
        if (item.LoaiHoaDon === 12) {
            temp = phieuChiTeamplate;
        }
        var itemHDFormat = GetInforPhieuThu(item);
        self.InforHDprintf(itemHDFormat);
        $.ajax({
            url: '/api/DanhMuc/ThietLapApi/GetContentFIlePrintTypeChungTu?maChungTu=' + temp + '&idDonVi=' + id_donvi,
            type: 'GET',
            dataType: 'json',
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            success: function (result) {
                var data = result;
                data = data.concat('<script src="/Scripts/knockout-3.4.2.js"></script>');
                data = data.concat("<script > var item1=[], item4 =[], item5 =[] ; var item2=[] ;var item3=" + JSON.stringify(itemHDFormat) + "; </script>");
                data = data.concat(" <script type='text/javascript' src='/Scripts/Thietlap/MauInTeamplate.js'></script>");
                PrintExtraReport(data);
            }
        });
    }
    function GetInforPhieuThu(objHD) {
        objHD.TenCuaHang = self.CongTy()[0].TenCongTy;
        objHD.DiaChiCuaHang = self.CongTy()[0].DiaChi;
        objHD.DienThoaiCuaHang = self.CongTy()[0].SoDienThoai;
        objHD.LogoCuaHang = Open24FileManager.hostUrl + self.CongTy()[0].DiaChiNganHang;
        objHD.ChiNhanhBanHang = objHD.TenChiNhanh;
        objHD.MaPhieu = objHD.MaHoaDon;
        objHD.NgayLapHoaDon = moment(objHD.NgayLapHoaDon).format('DD/MM/YYYY HH:mm:ss');
        objHD.NguoiNhanTien = objHD.NguoiNopTien;
        objHD.DiaChiKhachHang = self.ItemHoaDon().DiaChiKhachHang;
        objHD.DienThoaiKhachHang = self.ItemHoaDon().DienThoai;
        objHD.TienBangChu = DocSo(formatNumberToFloat(objHD.TongTienThu));
        objHD.GiaTriPhieu = formatNumber(objHD.TongTienThu);
        return objHD;
    }

    async function GetInforHDPrint(id, isDoiTraHang = false, arCT = null) {
        const hdDB = await GetInforHD_fromDB(id);
        const taiKhoanCK = await fetchBankAccountData(id);
        var objPrint = $.extend({}, hdDB);
        var phaiThanhToan = formatNumberToFloat(hdDB.PhaiThanhToan);
        var daThanhToan = RoundDecimal(hdDB.KhachDaTra, 0);
        if (objPrint.ID_PhieuTiepNhan != null) {
            objPrint.SoKmCu_PTN = await Gara_GetSoKmPTN(objPrint.ID_PhieuTiepNhan);
        }

        objPrint.MaHoaDonTraHang = objPrint.MaHoaDonGoc;
        objPrint.TenNhaCungCap = objPrint.TenDoiTuong;
        objPrint.DienThoaiKhachHang = objPrint.DienThoai;
        objPrint.TongTichDiem = formatNumber(objPrint.DiemSauGD);
        objPrint.NhanVienBanHang = objPrint.TenNhanVien;
        objPrint.TongGiamGia = formatNumber(hdDB.TongGiamGia + hdDB.KhuyeMai_GiamGia);

        let tongcong = formatNumberToFloat(objPrint.TongThanhToan);
        objPrint.NgayLapHoaDon = moment(hdDB.NgayLapHoaDon).format('DD/MM/YYYY HH:mm:ss');
        objPrint.Ngay = moment(hdDB.NgayLapHoaDon).format('DD');
        objPrint.Thang = moment(hdDB.NgayLapHoaDon).format('MM');
        objPrint.Nam = moment(hdDB.NgayLapHoaDon).format('YYYY');

        objPrint.TongTienHang = formatNumber3Digit(objPrint.TongTienHang);
        objPrint.PhaiThanhToan = formatNumber(phaiThanhToan);
        objPrint.DaThanhToan = formatNumber3Digit(daThanhToan, 0);
        objPrint.DiemGiaoDich = formatNumber(objPrint.DiemGiaoDich);
        objPrint.TienThua = 0;
        objPrint.TongTienThue = formatNumber(objPrint.TongTienThue);

        var conno = formatNumberToFloat(objPrint.PhaiThanhToan) - daThanhToan;
        if (isDoiTraHang) {
            // doi tra hang
            tongcong = tongcong - formatNumberToFloat(objPrint.TongTienTraHang);
            if (tongcong < 0) {
                // mua < tra
                objPrint.PhaiTraKhach = formatNumber(-tongcong);
                objPrint.TongCong = formatNumber(-tongcong);
                objPrint.PhaiThanhToan = 0;
            }
            else {
                // mua > tra --> khach phai tra
                objPrint.PhaiTraKhach = 0;
                objPrint.PhaiThanhToan = formatNumber(tongcong);
                objPrint.TongCong = formatNumber(tongcong);
            }
            objPrint.TongTienTraHang = formatNumber3Digit(hdDB.TongTienTraHang);
            objPrint.TongChiPhi = formatNumber(hdDB.PhiTraHang);
            objPrint.TongTienTra = hdDB.TongTienTraHang - hdDB.TongTienThue;
            objPrint.TongTienHoaDonMua = objPrint.TongTienHang;
        }
        else {
            // tra hang
            objPrint.TongTienTraHang = objPrint.TongTienHang;
            objPrint.TongChiPhi = formatNumber(objPrint.TongChiPhi);
            objPrint.TongCong = formatNumber(tongcong);
            objPrint.TongTienTra = formatNumber(tongcong);
        }

        if (hdDB.LoaiHoaDon === 6) {
            objPrint.TongTienTraHang = hdDB.TongTienHang;
            objPrint.TongChiPhi = formatNumber(hdDB.TongChiPhi);
            objPrint.TongChiPhiHangTra = formatNumber(hdDB.TongChiPhi);
            objPrint.TongCong = tongcong;
        }
        var notruoc = 0, nosau = 0, diachiKH = '', ngaysinhKH = '';
        let customer = {};
        if (!commonStatisJs.CheckNull(hdDB.ID_DoiTuong)) {
            customer = await GetInforCus(hdDB.ID_DoiTuong);
            if (!$.isEmptyObject(customer)) {
                notruoc = customer.NoHienTai - conno;
                diachiKH = customer.DiaChi;
                ngaysinhKH = customer.NgaySinh_NgayTLap;
            }
          /*  nosau = customer.NoHienTai;*/
            notruoc = notruoc < 0 ? 0 : notruoc;
        }
        else {
            nosau = conno;// khachle
        }

        if (!commonStatisJs.CheckNull(ngaysinhKH)) {
            objPrint.NgaySinh_NgayTLap = moment(ngaysinhKH).format('DD/MM/YYYY');
        }
        objPrint.DiaChiKhachHang = diachiKH;
        objPrint.NoTruoc = formatNumber(notruoc);
        objPrint.NoSau = formatNumber(nosau);
        objPrint.NoSau_BangChu = DocSo(nosau);
        objPrint.NgayIn = formatDateTime(new Date());
        objPrint.ChiPhiNhap = objPrint.TongChiPhi;
        objPrint.GhiChu = objPrint.DienGiai;
        let tc = RoundDecimal(formatNumberToFloat(objPrint.TongCong), 0);
        objPrint.TienBangChu = DocSo(tc);
        objPrint.TenChiNhanh = objPrint.TenDonVi; // chi nhanh ban hang
        // logo cong ty
        if (self.CongTy().length > 0) {
            objPrint.LogoCuaHang = Open24FileManager.hostUrl + self.CongTy()[0].DiaChiNganHang;
            objPrint.TenCuaHang = self.CongTy()[0].TenCongTy;
            objPrint.DiaChiCuaHang = self.CongTy()[0].DiaChi;
            objPrint.DienThoaiCuaHang = self.CongTy()[0].SoDienThoai;
        }

        objPrint.TienMat = formatNumber3Digit(objPrint.TienMat);
        objPrint.TienATM = formatNumber3Digit(objPrint.TienATM);// in store: assign TienGui = TienATM
        objPrint.TTBangTienCoc = formatNumber3Digit(hdDB.TienDatCoc);
        objPrint.TienKhachThieu = formatNumber(conno);
        objPrint.PhaiThanhToan_TruCocBG = hdDB.PhaiThanhToan - hdDB.ThuDatHang;
        objPrint.PhaiThanhToan_TruCoc = hdDB.PhaiThanhToan - hdDB.ThuDatHang - hdDB.TienDatCoc;
        objPrint.TienKhachThieu_BangChu = DocSo(conno);
        objPrint.KH_DaThanhToan_BangChu = DocSo(daThanhToan);
        objPrint.KH_DaThanhToan_TruCocBG = daThanhToan - hdDB.ThuDatHang;
        objPrint.KH_DaThanhToan_TruCocBG_BangChu = DocSo(daThanhToan - hdDB.ThuDatHang);

        // the gia tri
        objPrint.TongTaiKhoanThe = formatNumber(vmThanhToan.theGiaTriCus.TongNapThe);
        objPrint.TongSuDungThe = formatNumber(vmThanhToan.theGiaTriCus.SuDungThe);
        objPrint.SoDuConLai = formatNumber(vmThanhToan.theGiaTriCus.SoDuTheGiaTri);
        objPrint.TienDoiDiem = formatNumber(objPrint.TienDoiDiem);
        objPrint.TienTheGiaTri = formatNumber(objPrint.ThuTuThe);

        let pthuc = '';
        if (formatNumberToFloat(hdDB.TienMat) > 0) {
            pthuc += 'Tiền mặt, ';
        }
        if (formatNumberToFloat(hdDB.TienATM) > 0) {
            pthuc += 'POS, ';
        }
        if (formatNumberToFloat(hdDB.ChuyenKhoan) > 0) {
            pthuc += 'Chuyển khoản, ';
            let qrCode = await getQRCode({
                accountNo: taiKhoanCK.SoTaiKhoan,
                accountName: taiKhoanCK.TenChuThe,
                acqId: taiKhoanCK.MaPinNganHang,
                addInfo: 'Thanh Toan Hoa Don ' + hdDB.MaHoaDon,
                amount: taiKhoanCK.TienThu
            });
            objPrint.TenNganHangChuyenKhoan = taiKhoanCK.TenNganHang;
            objPrint.TenChuTheChuyenKhoan = taiKhoanCK.TenChuThe;
            objPrint.SoTaiKhoanChuyenKhoan = taiKhoanCK.SoTaiKhoan;
            if (qrCode != '') {
                objPrint.LinkQR = qrCode;
            }
        }
        if (formatNumberToFloat(hdDB.ThuTuThe) > 0) {
            pthuc += 'Thẻ giá trị, ';
            let param = {
                IDChiNhanhs: [hdDB.ID_DonVi],
                IDCustomers: [hdDB.ID_DoiTuong],
                DateFrom: '2016-01-01',
                DateTo: moment(new Date()).format('YYYY-MM-DD'),
                CurrentPage: 0,
                PageSize: 1000,
            }
            objPrint.TienTheGiaTri_TruocTT = await getHisUsedValueCard(param, hdDB.MaHoaDon)
        }
        if (formatNumberToFloat(hdDB.TienDoiDiem) > 0) {
            pthuc += 'Điểm, ';
        }
        if (formatNumberToFloat(hdDB.TienDatCoc) > 0) {
            pthuc += 'Tiền cọc, ';
        }
        objPrint.PhuongThucTT = Remove_LastComma(pthuc);

        let nvHoaDon = '';
        let nvHoaDon_inCK = '';
        if (objPrint.BH_NhanVienThucHiens !== null && objPrint.BH_NhanVienThucHiens.length > 0) {
            for (let i = 0; i < objPrint.BH_NhanVienThucHiens.length; i++) {
                let nv = objPrint.BH_NhanVienThucHiens[i];
                nvHoaDon += nv.TenNhanVien + ', ';
                nvHoaDon_inCK += nv.TenNhanVien.concat(nv.PT_ChietKhau > 0 ? ' ('.concat(nv.PT_ChietKhau, ' %)') : ' ('.concat(formatNumber(nv.TienChietKhau), ')'), ', ');
            }
        }
        objPrint.ChietKhauNVHoaDon = Remove_LastComma(nvHoaDon);
        objPrint.ChietKhauNVHoaDon_InGtriCK = Remove_LastComma(nvHoaDon_inCK);

        objPrint.TongSoLuongHang = formatNumber(self.TongSoLuongHang());
        objPrint.TongGiamGiaHang = formatNumber(self.TongGiamGiaHang());
        objPrint.TongTienDichVu = formatNumber(self.TongTienDichVu());
        objPrint.TongTienDichVu_TruocVAT = formatNumber(self.TongTienDichVu_TruocVAT());
        objPrint.TongTienDichVu_TruocCK = formatNumber(self.TongTienDichVu_TruocCK());
        objPrint.TongTienDichVu_TruocCK_SauVAT = self.TongTienDichVu_TruocCK() + self.TongThue_DichVu();

        objPrint.TongTienPhuTung = formatNumber(self.TongTienPhuTung());
        objPrint.TongTienPhuTung_TruocVAT = formatNumber(self.TongTienPhuTung_TruocVAT());
        objPrint.TongTienPhuTung_TruocCK = formatNumber(self.TongTienPhuTung_TruocCK());
        objPrint.TongTienPhuTung_TruocCK_SauVAT = self.TongTienPhuTung_TruocCK() + self.TongThue_PhuTung();
        objPrint.TongTienHangChuaCK = formatNumber(self.TongTienHangChuaCK());
        objPrint.TongGiamGiaHD_HH = formatNumber(self.TongGiamGiaHang() + hdDB.TongGiamGia + hdDB.KhuyeMai_GiamGia);
        objPrint.TongTienHDSauGiamGia = formatNumber3Digit(formatNumberToFloat(objPrint.TongTienHang) - objPrint.TongGiamGiaKM_HD);

        objPrint.TongThue_PhuTung = self.TongThue_PhuTung();
        objPrint.TongCK_PhuTung = self.TongCK_PhuTung();
        objPrint.TongThue_DichVu = self.TongThue_DichVu();
        objPrint.TongCK_DichVu = self.TongCK_DichVu();
        objPrint.TongSL_DichVu = self.TongSL_DichVu();
        objPrint.TongSL_PhuTung = self.TongSL_PhuTung();

        // gara
        objPrint.BH_TenLienHe = hdDB.LienHeBaoHiem;
        objPrint.BH_SDTLienHe = hdDB.SoDienThoaiLienHeBaoHiem;

        objPrint.TongTienBHDuyet = formatNumber(hdDB.TongTienBHDuyet);
        objPrint.KhauTruTheoVu = formatNumber(hdDB.KhauTruTheoVu);
        objPrint.GiamTruBoiThuong = formatNumber(hdDB.GiamTruBoiThuong);
        objPrint.BHThanhToanTruocThue = formatNumber(hdDB.BHThanhToanTruocThue);
        objPrint.TongTienThueBaoHiem = formatNumber3Digit(hdDB.TongTienThueBaoHiem, 0);
        objPrint.BH_TienBangChu = DocSo(hdDB.PhaiThanhToanBaoHiem);
        objPrint.KH_TienBangChu = DocSo(objPrint.PhaiThanhToan_TruCoc);
        objPrint.BH_ConThieu = formatNumber3Digit(hdDB.PhaiThanhToanBaoHiem - hdDB.BaoHiemDaTra);
        objPrint.BH_ConThieu_BangChu = DocSo(hdDB.PhaiThanhToanBaoHiem - hdDB.BaoHiemDaTra);

        if (commonStatisJs.CheckNull(hdDB.ID_PhieuTiepNhan)) {
            let cthd = [];
            if (arCT !== null) {
                cthd = $.grep(arCT, function (x) {
                    return !commonStatisJs.CheckNull(x.ID_Xe);
                });
            }
            else {
                cthd = $.grep(self.BH_HoaDonChiTiets(), function (x) {
                    return !commonStatisJs.CheckNull(x.ID_Xe);
                });
            }

            objPrint.BienSo = '';
            objPrint.ChuXe = '';
            objPrint.ChuXe_DiaChi = '';
            objPrint.HopSo = '';
            objPrint.MauSon = '';
            objPrint.NamSanXuat = '';
            objPrint.SoKhung = '';
            objPrint.SoMay = '';
            objPrint.TenHangXe = '';
            objPrint.TenLoaiXe = '';
            objPrint.TenMauXe = '';

            if (cthd.length > 0) {
                let car = await GetInforCar_ByID(cthd[0].ID_Xe);
                if (!$.isEmptyObject(car)) {
                    objPrint.BienSo = car.BienSo;
                    objPrint.ChuXe = car.TenDoiTuong;
                    objPrint.ChuXe_DiaChi = car.ChuXe_DiaChi;
                    objPrint.HopSo = car.HopSo;
                    objPrint.MauSon = car.MauSon;
                    objPrint.NamSanXuat = car.NamSanXuat;
                    objPrint.SoKhung = car.SoKhung;
                    objPrint.SoMay = car.SoMay;
                    objPrint.TenHangXe = car.TenHangXe;
                    objPrint.TenLoaiXe = car.TenLoaiXe;
                    objPrint.TenMauXe = car.TenMauXe;
                }
            }
        }
        else {
            // phieutiepnhan
            let tn = self.ThongTinPhieuTiepNhan();
            if (tn) {
                objPrint.BienSo = tn.BienSo;
                objPrint.ChuXe = tn.ChuXe;
                objPrint.ChuXe_DiaChi = tn.ChuXe_DiaChi;
                objPrint.ChuXe_Email = tn.ChuXe_Email;
                objPrint.ChuXe_SDT = tn.ChuXe_SDT;
                objPrint.CoVanDichVu = tn.CoVanDichVu;
                objPrint.CoVan_SDT = tn.CoVan_SDT;
                objPrint.DungTich = tn.DungTich;
                objPrint.PTN_GhiChu = tn.GhiChu;
                objPrint.HopSo = tn.HopSo;
                objPrint.NhanVienTiepNhan = tn.NhanVienTiepNhan;
                objPrint.MauSon = tn.MauSon;
                objPrint.NamSanXuat = tn.NamSanXuat;
                objPrint.NgayVaoXuong = moment(tn.NgayVaoXuong).format('DD/MM/YYYY HH:mm');
                objPrint.NgayXuatXuong = tn.NgayXuatXuong ? moment(tn.NgayXuatXuong).format('DD/MM/YYYY HH:mm') : '';
                objPrint.NgayXuatXuongDuKien = tn.NgayXuatXuongDuKien ? moment(tn.NgayXuatXuongDuKien).format('DD/MM/YYYY HH:mm') : '';
                objPrint.SoDienThoaiLienHe = tn.SoDienThoaiLienHe;
                objPrint.SoKhung = tn.SoKhung;
                objPrint.SoMay = tn.SoMay;
                objPrint.SoKmRa = tn.SoKmRa;
                objPrint.SoKmVao = tn.SoKmVao;
                objPrint.TenHangXe = tn.TenHangXe;
                objPrint.TenLoaiXe = tn.TenLoaiXe;
                objPrint.TenMauXe = tn.TenMauXe;
                objPrint.TenLienHe = tn.TenLienHe;
                objPrint.ID_ChuXe = tn.ID_ChuXe;
            }
        }
        return objPrint;
    }

    async function getHisUsedValueCard(param, maHoaDon) {

        let response = await $.ajax({
            url: '/api/DanhMuc/DM_DoiTuongAPI/GetListHisUsed_ValueCard',
            type: 'POST',
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            dataType: 'json',
            data: param,
        });

        const sanitizedMaHoaDon = maHoaDon.replace(/[, ]/g, "");
        const soDuTruoc = response.dataSoure.data.find(item =>
            item.MaHoaDon.replace(/[, ]/g, "") === sanitizedMaHoaDon
        )?.SoDuTruoc || 0;
        return soDuTruoc;
    }
    function GetInforCongTy() {
        ajaxHelper('/api/DanhMuc/HT_API/' + 'GetHT_CongTy', 'GET').done(function (data) {
            if (data !== null) {
                self.CongTy(data);
            }
        });
    }
    async function GetCTHDPrint_Format(idHoaDon) {
        const cthdDB = await GetChiTietHD_fromDB(idHoaDon);
        const allCombo = await vmThanhPhanCombo.GetAllCombo_byIDHoaDon(idHoaDon);

        Caculator_ChiTietHD(cthdDB);

        var arrCTHD = [];
        var thuocTinh = '';
        for (let i = 0; i < cthdDB.length; i++) {
            let itFor = $.extend({}, cthdDB[i]);
            let price = formatNumberToFloat(itFor.DonGia);
            let sale = formatNumberToFloat(itFor.GiamGia);
            let tienThue = formatNumberToFloat(itFor.TienThue);
            let giaban = formatNumberToFloat(itFor.GiaBan);
            let bh_tt = itFor.SoLuong * formatNumberToFloat(itFor.DonGiaBaoHiem);

            thuocTinh = itFor.ThuocTinh_GiaTri;
            thuocTinh = thuocTinh === null || thuocTinh === '' ? '' : thuocTinh.substr(1);
            itFor.DonGia = formatNumber(price);
            itFor.DonGiaBaoHiem = formatNumber(itFor.DonGiaBaoHiem);
            itFor.BH_ThanhTien = formatNumber3Digit(bh_tt);
            itFor.TienChietKhau = formatNumber(sale);
            itFor.GiaBan = formatNumber(giaban);
            itFor.GiaBanSauVAT = giaban + tienThue; // dongia - giamgia + vat
            itFor.DonGiaSauVAT = price + tienThue;// dongia + vat
            itFor.SoLuong = formatNumber3Digit(itFor.SoLuong);
            itFor.ThanhTien = formatNumber3Digit(itFor.ThanhTien);
            itFor.ThuocTinh_GiaTri = thuocTinh;

            let sophutTH = itFor.ThoiGianThucHien;
            let quathoigian = itFor.QuaThoiGian;
            if (sophutTH > 0) {
                itFor.TimeStart = moment(itFor.ThoiGian).format('HH:mm');
                itFor.ThoiGianThucHien = sophutTH + ' phút';
                itFor.QuaThoiGian = quathoigian + ' phút';
            }

            // nvthuchien, tuvan co in %ck 
            let th_CoCK = '';
            let tv_CoCK = '';
            itFor = AssignNVThucHien_toCTHD(itFor);
            if (itFor.BH_NhanVienThucHien != null) {
                for (let j = 0; j < itFor.BH_NhanVienThucHien.length; j++) {
                    let for2 = itFor.BH_NhanVienThucHien[j];
                    if (for2.ThucHien_TuVan) {
                        th_CoCK += for2.TenNhanVien.concat(for2.PT_ChietKhau > 0 ? ' ('.concat(for2.PT_ChietKhau, ' %)') : ' ('.concat(formatNumber(for2.TienChietKhau), ')'), ', ');
                    }
                    else {
                        tv_CoCK += for2.TenNhanVien.concat(for2.PT_ChietKhau > 0 ? ' ('.concat(for2.PT_ChietKhau, ' %)') : ' ('.concat(formatNumber(for2.TienChietKhau), ')'), ', ');
                    }
                }
            }
            itFor.NVThucHienDV_CoCK = Remove_LastComma(th_CoCK);
            itFor.NVTuVanDV_CoCK = Remove_LastComma(tv_CoCK);

            let lstCombo = $.grep(allCombo, function (x) {
                return x.ID_ParentCombo === itFor.ID_ParentCombo;
            });
            if (lstCombo.length > 0) {
                itFor.ThanhPhanComBo = lstCombo;
                itFor = AssignThanhPhanComBo_toCTHD(itFor);
            }
            else {
                itFor.ThanhPhanComBo = [];
            }

            itFor.NgaySanXuat = '';
            itFor.NgayHetHan = '';
            if (!commonStatisJs.CheckNull(cthdDB[i].NgaySanXuat)) {
                itFor.NgaySanXuat = moment(cthdDB[i].NgaySanXuat).format('DD/MM/YYYY');
            }
            if (!commonStatisJs.CheckNull(cthdDB[i].NgayHetHan)) {
                itFor.NgayHetHan = moment(cthdDB[i].NgayHetHan).format('DD/MM/YYYY');
            }
            arrCTHD.push(itFor);
        }

        arrCTHD = arrCTHD.sort(function (a, b) {
            let x = a.SoThuTu,
                y = b.SoThuTu;
            return x < y ? -1 : x > y ? 1 : 0;
        });
        return arrCTHD;
    }

    self.XuLiDonHang = async function (item) {
        localStorage.setItem('createHDfrom', 2);

        const hdDB = await GetInforHD_fromDB(item.ID);
        if ($.isEmptyObject(hdDB)) return;

        var phaiTT = hdDB.PhaiThanhToan - hdDB.KhachDaTra;
        var dvtGiam = 'VND';
        if (item.TongChietKhau > 0) {
            dvtGiam = '%';
        }
        let objTax = GetPTChietKhauHang_HeaderBH();
        const ngayLapHD = self.NganhKinhDoanh() === 2 ? moment(hdDB.NgayLapHoaDon).format('YYYY-MM-DD HH:mm') : moment(hdDB.NgayLapHoaDon).format('YYYY-MM-DD HH:mm:ss');
        var itemNew = {
            ID: item.ID,
            ID_HoaDon: null,
            ID_DonVi: hdDB.ID_DonVi,
            MaHoaDon: hdDB.MaHoaDon,
            MaHoaDonDB: hdDB.MaHoaDon,
            LoaiHoaDon: loaiHoaDon,
            ID_DoiTuong: hdDB.ID_DoiTuong,
            NguoiTao: userLogin,
            ID_BangGia: hdDB.ID_BangGia,
            BangGiaWasChanged: false,
            ID_NhanVien: hdDB.ID_NhanVien,
            NgayLapHoaDon: ngayLapHD,
            TongTienHang: hdDB.TongTienHang,
            PhaiThanhToan: hdDB.PhaiThanhToan,
            TongGiamGia: hdDB.TongGiamGia,
            KhachDaTra: hdDB.KhachDaTra,
            PTThueHoaDon: hdDB.PTThueHoaDon,
            TongTienThue: hdDB.TongTienThue,
            PTThueBaoHiem: 0,
            TongTienThueBaoHiem: 0,
            ChoThanhToan: false,
            DienGiai: hdDB.DienGiai,
            TienGui: 0,
            TienThua: 0,
            Status: 1,
            StatusOffline: false,
            DVTinhGiam: dvtGiam,
            TongChietKhau: hdDB.TongChietKhau, // PTGiam
            // gan DaThanhToan : Tien con lai phai TT --> to do bind in BanLe.js
            DaThanhToan: self.IsGara() ? 0 : phaiTT,
            // tien khach con phai TT : tong PhaiTT - KhachDaTra
            PhaiThanhToan: self.IsGara() ? hdDB.PhaiThanhToan : phaiTT,
            TienMat: self.IsGara() ? 0 : phaiTT,
            TienATM: 0,
            PhaiThanhToanBaoHiem: 0,
            BaoHiemDaTra: 0,
            // Tra Hang
            PhaiThanhToanDB: 0,
            TongGiaGocHangTra: 0,
            TongChiPhiHangTra: 0,
            TongChiPhi: hdDB.TongChiPhi,
            HoanTraThuKhac: 0,
            TongTienTra: 0,
            PhaiTraKhach: 0,
            DaTraKhach: 0,
            GiaoHang: false,
            TongGiamGiaDB: hdDB.TongGiamGia,
            DiemGiaoDichDB: 0,
            PTGiamDB: 0,
            TongTienMua: 0,
            PTThueDB: 0,
            TongThueDB: 0,
            // DatHang
            TrangThaiHD: 6, // HD DatHang dang xuly
            IsActive: '',
            YeuCau: hdDB.YeuCau,
            IsChose: true, // tức là đang thao tác với hóa đơn này
            HoanTraTamUng: 0,
            IsKhuyenMaiHD: false,
            IsOpeningKMaiHD: false,
            KhuyeMai_GiamGia: 0,
            TongGiamGiaKM_HD: hdDB.TongGiamGia + hdDB.KhuyeMai_GiamGia, // to do bind GiamGia at BanHang
            // Tich Diem
            TTBangDiem: 0,
            DiemGiaoDich: 0,
            DiemQuyDoi: 0,
            DiemHienTai: 0,
            DiemCong: 0,  // use when KM_Cong diem
            DiemKhuyenMai: 0,

            ID_NhomDTApplySale: null,
            // Goi dich vu
            NgayApDungGoiDV: null,
            HanSuDungGoiDV: null,
            CreateTime: 0,
            ID_ViTri: null,
            TenViTriHD: '',
            BH_NhanVienThucHiens: [],
            TienTheGiaTri: 0,
            ThoiGianThucHien: 0,

            MaPhieuTiepNhan: hdDB.MaPhieuTiepNhan,
            BienSo: hdDB.BienSo,
            ID_PhieuTiepNhan: hdDB.ID_PhieuTiepNhan,
            ID_BaoHiem: hdDB.ID_BaoHiem,
            LienHeBaoHiem: hdDB.LienHeBaoHiem,
            SoDienThoaiLienHeBaoHiem: hdDB.SoDienThoaiLienHeBaoHiem,
            PhaiThanhToanBaoHiem: 0,
            ChiPhi_GhiChu: hdDB.ChiPhi_GhiChu,
            TongThanhToan: hdDB.TongThanhToan,
            TenBaoHiem: hdDB.TenBaoHiem,
            XuatKhoAll: false,
            DuyetBaoGia: !hdDB.ChoThanhToan,
            DuyetBaoHanh: !hdDB.ChoThanhToan,

            PTChietKhauHH: objTax.PTChietKhauHH,
            TongGiamGiaHang: self.TongGiamGiaHang(),
            TongTienHangChuaCK: self.TongTienHangChuaCK(),
            TongTienKhuyenMai_CT: 0,
            TongGiamGiaKhuyenMai_CT: 0,

            TongTienBHDuyet: 0,
            GiamTruBoiThuong: 0,
            SoVuBaoHiem: '',
            KhauTruTheoVu: 0,
            PTGiamTruBoiThuong: 0,
            BHThanhToanTruocThue: 0,
            TongThueKhachHang: hdDB.TongTienThue,
            CongThucBaoHiem: 13,
            GiamTruThanhToanBaoHiem: 0,
            HeaderBH_GiaTriPtram: 0,
            HeaderBH_Type: 1,
        }

        // save cache lcXuLiDonHang to arrayJson --> xu li nhieu DH cung 1 luc
        var lcXuLiDonHang = localStorage.getItem('lcXuLiDonHang');
        if (lcXuLiDonHang === null) {
            lcXuLiDonHang = [];
        }
        else {
            lcXuLiDonHang = JSON.parse(lcXuLiDonHang);
            // remove and add new
            lcXuLiDonHang = $.grep(lcXuLiDonHang, function (x) {
                return x.MaHoaDon !== hdDB.MaHoaDon;
            });
        }

        const allComboHD = await vmThanhPhanCombo.GetAllCombo_byIDHoaDon(item.ID);

        ajaxHelper(BH_HoaDonUri + 'GetCTHoaDon_afterDatHang?idHoaDon=' + item.ID, 'GET').done(function (data) {
            if (data !== null) {
                // order by SoThuTu ASC --> group Hang Hoa by LoHang
                var arrCTsort = data.sort(function (a, b) {
                    var x = a.SoThuTu,
                        y = b.SoThuTu;
                    return x < y ? -1 : x > y ? 1 : 0;
                });

                var lcCTDatHang = localStorage.getItem('lcCTDatHang');
                if (lcCTDatHang === null) {
                    lcCTDatHang = [];
                }
                else {
                    lcCTDatHang = JSON.parse(lcCTDatHang);
                    lcCTDatHang = $.grep(lcCTDatHang, function (x) {
                        return x.MaHoaDon !== hdDB.MaHoaDon;
                    })
                }

                var ctConLai = $.grep(arrCTsort, function (x) {
                    return x.SoLuongConLai > 0;
                });
                if (ctConLai.length === 0) {
                    commonStatisJs.ShowMessageDanger('Đã xử lý hết báo giá');
                    return;
                }

                lcXuLiDonHang.push(itemNew);// chi luu cache neu soluongConLai > 0

                var arrIDQuiDoi1 = [];
                var cthdLoHang = [];
                for (let i = 0; i < arrCTsort.length; i++) {
                    let ctNew = $.extend({}, arrCTsort[i]);
                    ctNew = SetDefaultPropertiesCTHD(arrCTsort[i], hdDB.MaHoaDon, hdDB.LoaiHoaDon);
                    ctNew.SoLuongDaMua = 0;
                    ctNew.TienChietKhau = ctNew.GiamGia;
                    ctNew.DVTinhGiam = '%';
                    ctNew.GiaBan = ctNew.DonGia;
                    ctNew.ThanhTien = (arrCTsort[i].GiaBan - ctNew.TienChietKhau) * ctNew.SoLuong;
                    if (arrCTsort[i].TienChietKhau > 0) {
                        if (arrCTsort[i].PTChietKhau === 0) {
                            ctNew.DVTinhGiam = 'VND';
                        }
                    }
                    ctNew.CssWarning = false;
                    ctNew.ChatLieu = '3';
                    ctNew.ID_ChiTietGoiDV = null;
                    ctNew.GhiChu_NVThucHien = '';
                    ctNew.BH_NhanVienThucHien = [];// dathang: khong co NVThucHien
                    ctNew.GhiChu_NVTuVan = '';
                    ctNew.GhiChu_NVThucHienPrint = '';
                    ctNew.GhiChu_NVTuVanPrint = '';
                    let quycach = ctNew.QuyCach === null || ctNew.QuyCach === 0 ? 1 : ctNew.QuyCach;
                    ctNew.QuyCach = quycach;
                    // DatHang: phi DV = 0
                    ctNew.ID_ViTri = null;
                    ctNew.TenViTri = '';
                    ctNew.ThoiGianThucHien = 0;

                    ctNew = AssignTPDinhLuong_toCTHD(ctNew);
                    ctNew = AssignNVThucHien_toCTHD(ctNew);

                    let quanLiTheoLo = ctNew.QuanLyTheoLoHang;
                    ctNew.DM_LoHang = [];
                    ctNew.LotParent = quanLiTheoLo;

                    let dateLot = GetNgaySX_NgayHH(ctNew);
                    ctNew.NgaySanXuat = dateLot.NgaySanXuat;
                    ctNew.NgayHetHan = dateLot.NgayHetHan;

                    // get tpcombo
                    let combo = $.grep(allComboHD, function (x) {
                        return x.ID_ParentCombo === ctNew.ID_ParentCombo;
                    });
                    if (combo.length > 0) {
                        ctNew.ThanhPhanComBo = combo;
                        ctNew = AssignThanhPhanComBo_toCTHD(ctNew);
                    }
                    else {
                        ctNew.ThanhPhanComBo = [];
                    }

                    // check exist in cthdLoHang
                    if ($.inArray(arrCTsort[i].ID_DonViQuiDoi, arrIDQuiDoi1) === -1) {
                        arrIDQuiDoi1.unshift(arrCTsort[i].ID_DonViQuiDoi);
                        // push CTHD
                        ctNew.SoThuTu = cthdLoHang.length + 1;
                        ctNew.IDRandom = CreateIDRandom('RandomCT_');
                        if (quanLiTheoLo) {
                            // push DM_Lo
                            let objLot = $.extend({}, ctNew);
                            objLot.DM_LoHang = [];
                            objLot.HangCungLoais = [];
                            ctNew.DM_LoHang.push(objLot);
                        }
                        cthdLoHang.push(ctNew);
                    }
                    else {
                        // find in cthdLoHang with same ID_QuiDoi
                        for (let j = 0; j < cthdLoHang.length; j++) {
                            if (cthdLoHang[j].ID_DonViQuiDoi === ctNew.ID_DonViQuiDoi) {
                                if (quanLiTheoLo) {
                                    let objLot = $.extend({}, ctNew);
                                    objLot.LotParent = false;
                                    objLot.IDRandom = CreateIDRandom('RandomCT_');
                                    objLot.DM_LoHang = [];
                                    objLot.HangCungLoais = [];
                                    cthdLoHang[j].DM_LoHang.push(objLot);
                                }
                                else {
                                    ctNew.IDRandom = CreateIDRandom('RandomCT_');
                                    ctNew.LaConCungLoai = true;
                                    cthdLoHang[j].HangCungLoais.push(ctNew);
                                }
                                break;
                            }
                        }
                    }
                }
                // sort CTHD by SoThuTu desc
                cthdLoHang = cthdLoHang.sort(function (a, b) {
                    var x = a.SoThuTu, y = b.SoThuTu;
                    return x < y ? 1 : x > y ? -1 : 0;
                });
                // push in cache CTHD (xu li nhieu CT cung luc)
                for (let i = 0; i < cthdLoHang.length; i++) {
                    lcCTDatHang.push(cthdLoHang[i]);
                }

                localStorage.setItem('lcXuLiDonHang', JSON.stringify(lcXuLiDonHang));
                localStorage.setItem('lcCTDatHang', JSON.stringify(lcCTDatHang));

                CheckExistCacheHD_andRemove(item.ID);
                SetCache_ifGara('TN_xulyBG');
                if (self.IsGara()) {
                    localStorage.setItem('maHDCache', hdDB.MaHoaDon);// used to get at gara.js (phieu dang xuly)
                }
                self.gotoGara();
            }
            else {
                ShowMessage_Danger('Không có dữ liệu');
                return false;
            }
        });
    }
    self.DatHang = function () {
        localStorage.setItem('fromDatHang', true);
        self.gotoGara();
    }
    self.AdđLenhBH = function () {
        localStorage.setItem('fromLenhBH', true);
        self.gotoGara();
    }

    self.DuyetBaoGia = function (item) {
        var $this = $(event.currentTarget);
        ajaxHelper('/api/DanhMuc/GaraAPI/' + 'Duyet_HuyBaoGia?id=' + item.ID + '&trangthai=' + false, 'GET').done(function (x) {

            if (x.res) {
                ShowMessage_Success('Đã duyệt báo giá thành công');
                // savediary
                var diary = {
                    ID_DonVi: id_donvi,
                    ID_NhanVien: _id_NhanVien,
                    ChucNang: 'Danh sách báo giá',
                    LoaiNhatKy: 1,
                    NoiDung: 'Duyệt báo giá '.concat(item.MaHoaDon),
                    NoiDungChiTiet: 'Duyệt báo giá '.concat(' báo giá ', item.MaHoaDon,
                        ', người duyệt: ', userLogin),
                }
                Insert_NhatKyThaoTac_1Param(diary);

                $this.next().show();
                $this.hide();
            }
            else {
                ShowMessage_Danger('Duyệt báo giá thất bại');
            }
        })
    }

    self.CopyDatHang = async function (item) {
        localStorage.setItem('createHDfrom', 1);
        SetCache_ifGara('TN_copyDH');

        const hdDB = await GetInforHD_fromDB(item.ID);
        if ($.isEmptyObject(hdDB)) return;

        var phaiTT = hdDB.PhaiThanhToan;
        var dvtGiam = 'VND';
        if (item.TongChietKhau > 0) {
            dvtGiam = '%';
        }

        var obj = GetPTChietKhauHang_HeaderBH();
        var itemNew = {
            ID: const_GuidEmpty,
            ID_HoaDon: null,
            MaHoaDon: hdDB.MaHoaDon,
            MaHoaDonDB: 'Copy' + hdDB.MaHoaDon,
            LoaiHoaDon: loaiHoaDon,
            ID_DonVi: hdDB.ID_DonVi,
            ID_DoiTuong: hdDB.ID_DoiTuong,
            ID_BangGia: hdDB.ID_BangGia,
            ID_NhanVien: hdDB.ID_NhanVien,
            NguoiTao: userLogin,
            NgayLapHoaDon: null, // assign = null: auto update time

            TongTienHang: hdDB.TongTienHang,
            TongGiamGia: hdDB.TongGiamGia,
            KhachDaTra: 0,
            PTThueHoaDon: hdDB.PTThueHoaDon,
            TongTienThue: hdDB.TongTienThue,
            PTThueBaoHiem: 0,
            TongTienThueBaoHiem: 0,
            ChoThanhToan: false,
            DienGiai: hdDB.DienGiai,
            TienGui: 0,
            TienATM: 0,
            TienThua: 0,
            Status: 1,
            StatusOffline: false,
            DVTinhGiam: dvtGiam,
            TongChietKhau: hdDB.TongChietKhau, // PTGiam
            DaThanhToan: 0,
            PhaiThanhToan: self.IsGara() ? hdDB.PhaiThanhToan : phaiTT,
            TienMat: self.IsGara() ? 0 : phaiTT,
            // Tra Hang
            TongGiaGocHangTra: 0,
            TongChiPhiHangTra: 0,
            TongChiPhi: hdDB.TongChiPhi,
            HoanTraThuKhac: 0,
            TongTienTra: 0,
            PhaiTraKhach: 0,
            DaTraKhach: 0,
            GiaoHang: false,
            PhaiThanhToanDB: 0,
            TongGiamGiaDB: 0,
            DiemGiaoDichDB: 0,
            PTGiamDB: 0,
            TongTienMua: 0,
            PTThueDB: 0,
            TongThueDB: 0,

            TrangThaiHD: 1,// hd new
            IsActive: '',
            YeuCau: "1",
            IsChose: true, // chỉ sử dụng ở Xử lí đơn hàng
            HoanTraTamUng: 0,
            IsKhuyenMaiHD: false,
            IsOpeningKMaiHD: false,
            KhuyeMai_GiamGia: 0,
            TongGiamGiaKM_HD: hdDB.TongGiamGia + hdDB.KhuyeMai_GiamGia,
            // TichDiem
            TTBangDiem: 0,
            DiemGiaoDich: 0,
            DiemQuyDoi: 0,
            DiemHienTai: 0,
            // use when KM_Cong diem
            DiemCong: 0,
            ID_NhomDTApplySale: null,
            // Goi dich vu
            NgayApDungGoiDV: null,
            HanSuDungGoiDV: null,
            CreateTime: 0,
            ID_ViTri: null,
            TenViTriHD: '',
            BH_NhanVienThucHiens: [],
            TienTheGiaTri: 0,
            ThoiGianThucHien: 0,

            MaPhieuTiepNhan: hdDB.MaPhieuTiepNhan,
            BienSo: hdDB.BienSo,
            ID_PhieuTiepNhan: hdDB.ID_PhieuTiepNhan,
            ID_BaoHiem: hdDB.ID_BaoHiem,
            LienHeBaoHiem: hdDB.LienHeBaoHiem,
            SoDienThoaiLienHeBaoHiem: hdDB.SoDienThoaiLienHeBaoHiem,
            PhaiThanhToanBaoHiem: 0,
            ChiPhi_GhiChu: hdDB.ChiPhi_GhiChu,
            TongThanhToan: hdDB.TongThanhToan,
            TenBaoHiem: hdDB.TenBaoHiem,
            XuatKhoAll: false,
            DuyetBaoGia: false,

            PTChietKhauHH: obj.PTChietKhauHH,
            TongGiamGiaHang: self.TongGiamGiaHang(),
            TongTienHangChuaCK: self.TongTienHangChuaCK(),
            TongTienKhuyenMai_CT: 0,
            TongGiamGiaKhuyenMai_CT: 0,
            SoDuDatCoc: 0,

            TongTienBHDuyet: 0,
            GiamTruBoiThuong: 0,
            SoVuBaoHiem: '',
            KhauTruTheoVu: 0,
            PTGiamTruBoiThuong: 0,
            BHThanhToanTruocThue: 0,
            TongThueKhachHang: hdDB.TongTienThue,
            CongThucBaoHiem: 0,
            GiamTruThanhToanBaoHiem: 0,
            HeaderBH_GiaTriPtram: 0,
            HeaderBH_Type: 1,
        }
        localStorage.setItem('lcCopyDatHang', JSON.stringify(itemNew));

        const cthdDB = await GetChiTietHD_fromDB(item.ID);
        const allComboHD = await vmThanhPhanCombo.GetAllCombo_byIDHoaDon(item.ID);

        // order by SoThuTu ASC --> group Hang Hoa by LoHang
        var arrCTsort = cthdDB.sort(function (a, b) {
            var x = a.SoThuTu,
                y = b.SoThuTu;
            return x < y ? -1 : x > y ? 1 : 0;
        });
        var arrIDQuiDoi = [];
        var cthdLoHang = [];
        for (let i = 0; i < arrCTsort.length; i++) {
            let ctNew = $.extend({}, arrCTsort[i]);
            ctNew = SetDefaultPropertiesCTHD(ctNew, hdDB.MaHoaDon, 3);
            ctNew.SoLuongDaMua = 0;
            ctNew.TienChietKhau = ctNew.GiamGia;
            ctNew.DVTinhGiam = '%';
            ctNew.GiaBan = ctNew.DonGia;
            ctNew.ThanhTien = (ctNew.GiaBan - ctNew.TienChietKhau) * ctNew.SoLuong;
            if (ctNew.TienChietKhau > 0 && ctNew.PTChietKhau === 0) {
                ctNew.DVTinhGiam = 'VND';
            }
            ctNew.CssWarning = false;
            ctNew.GhiChu_NVThucHien = '';
            ctNew.BH_NhanVienThucHien = [];
            ctNew.GhiChu_NVTuVan = '';
            ctNew.GhiChu_NVThucHienPrint = '';
            ctNew.GhiChu_NVTuVanPrint = '';
            ctNew.ID_ChiTietGoiDV = null;
            ctNew.ChatLieu = '';
            ctNew.LaPTPhiDichVu = false;
            ctNew.PhiDichVu = 0;
            ctNew.TongPhiDichVu = 0;
            ctNew.ID_ViTri = null;
            ctNew.TenViTri = '';

            ctNew = AssignTPDinhLuong_toCTHD(ctNew);
            ctNew = AssignNVThucHien_toCTHD(ctNew);

            // lo hang
            let quanLiTheoLo = ctNew.QuanLyTheoLoHang;
            ctNew.DM_LoHang = [];
            ctNew.LotParent = quanLiTheoLo;

            let dateLot = GetNgaySX_NgayHH(ctNew);
            ctNew.NgaySanXuat = dateLot.NgaySanXuat;
            ctNew.NgayHetHan = dateLot.NgayHetHan;

            // get tpcombo
            let combo = $.grep(allComboHD, function (x) {
                return x.ID_ParentCombo === ctNew.ID_ParentCombo;
            });
            if (combo.length > 0) {
                ctNew.ThanhPhanComBo = combo;
                ctNew = AssignThanhPhanComBo_toCTHD(ctNew);
            }
            else {
                ctNew.ThanhPhanComBo = [];
            }

            // check exist in cthdLoHang
            if ($.inArray(ctNew.ID_DonViQuiDoi, arrIDQuiDoi) === -1) {
                arrIDQuiDoi.unshift(ctNew.ID_DonViQuiDoi);

                ctNew.SoThuTu = cthdLoHang.length + 1;
                ctNew.IDRandom = CreateIDRandom('RandomCT_');
                if (quanLiTheoLo) {
                    // push DM_Lo
                    let objLot = $.extend({}, ctNew);
                    objLot.DM_LoHang = [];
                    objLot.HangCungLoais = [];
                    ctNew.DM_LoHang.push(objLot);
                }
                cthdLoHang.push(ctNew);
            }
            else {
                // find in cthdLoHang with same ID_QuiDoi
                for (let j = 0; j < cthdLoHang.length; j++) {
                    if (cthdLoHang[j].ID_DonViQuiDoi === ctNew.ID_DonViQuiDoi) {
                        if (quanLiTheoLo) {
                            // push DM_Lo
                            let objLot = $.extend({}, ctNew);
                            objLot.LotParent = false;
                            objLot.IDRandom = CreateIDRandom('RandomCT_');
                            objLot.DM_LoHang = [];
                            objLot.HangCungLoais = [];
                            cthdLoHang[j].DM_LoHang.push(objLot);
                        }
                        else {
                            ctNew.IDRandom = CreateIDRandom('RandomCT_');
                            ctNew.LaConCungLoai = true;
                            cthdLoHang[j].HangCungLoais.push(ctNew);
                        }
                        break;
                    }
                }
            }
        }
        // sort CTHD by SoThuTu desc
        cthdLoHang = cthdLoHang.sort(function (a, b) {
            var x = a.SoThuTu, y = b.SoThuTu;
            return x < y ? 1 : x > y ? -1 : 0;
        });
        localStorage.setItem('lcCTDatHangCopy', JSON.stringify(cthdLoHang));
        self.gotoGara();
    }

    function SetDefaultPropertiesCTHD(itemCT, mahoadon, loaiHD) {
        itemCT.MaHoaDon = mahoadon;
        itemCT.LoaiHoaDon = loaiHD;
        itemCT.SrcImage = null;
        itemCT.CssWarning = loaiHD === 1 ? itemCT.SoLuong > itemCT.TonKho : false;
        itemCT.IsKhuyenMai = false;
        itemCT.IsOpeningKMai = false;
        itemCT.TenKhuyenMai = '';
        itemCT.HangHoa_KM = [];
        itemCT.DiemKhuyenMai = 0;
        itemCT.UsingService = false;
        itemCT.ListDonViTinh = [];
        itemCT.ShowEditQuyCach = false;
        itemCT.ShowWarningQuyCach = false;
        itemCT.SoLuongQuyCach = 0;
        itemCT.HangCungLoais = [];
        itemCT.ThanhPhanComBo = [];
        itemCT.LaConCungLoai = false;
        itemCT.TimeStart = 0;
        itemCT.QuaThoiGian = 0;
        itemCT.TimeRemain = 0;
        itemCT.ThoiGian = moment(new Date()).format('YYYY-MM-DD HH:mm');
        if (!commonStatisJs.CheckNull(itemCT.DonViTinh)) {
            itemCT.ListDonViTinh = itemCT.DonViTinh;
        }
        return itemCT;
    }

    function SetCache_ifGara(cacheName) {
        if (self.IsGara()) {
            localStorage.setItem('gara_CreateFrom', cacheName);
        }
    }

    function SetCache_ifNotGara(val) {
        if (!self.IsGara()) {
            localStorage.setItem('createHDfrom', val);
        }
    }
    self.SaoChepTraHang = async function (item) {
        if (item.TongTienHDDoiTra > 0) {
            ShowMessage_Danger('Hóa đơn có hàng đổi trả, không thể sao chép');
            return;
        }
        var hdNew = $.extend({}, item);
        // assign again ID = constGuid_Empty at BanLe.js
        SetCache_ifNotGara(4);
        SetCache_ifGara('TN_copyTH');
        hdNew.MaHoaDonDB = 'Copy' + hdNew.MaHoaDon;
        hdNew.Status = 1;
        hdNew.LoaiHoaDon = loaiHoaDon;
        hdNew.NgayLapHoaDon = null; // assign = null: auto update time
        hdNew.StatusOffline = false;
        hdNew.DVTinhGiam = 'VND';
        hdNew.NguoiTao = userLogin;
        if (hdNew.TongChietKhau > 0) {
            hdNew.DVTinhGiam = '%';
        }
        // Goi dich vu
        hdNew.CreateTime = 0; // bắt đầu chọn bàn (phòng) lúc HH:mm
        hdNew.TenViTriHD = '';
        hdNew.BH_NhanVienThucHiens = [];
        hdNew.TienTheGiaTri = 0;
        hdNew.DiemGiaoDichDB = 0;

        hdNew.ThoiGianThucHien = 0;
        hdNew.TrangThaiHD = 1;// saochep
        hdNew.YeuCau = 1;
        hdNew.ChoThanhToan = false;
        hdNew.DiemKhuyenMai = 0;
        hdNew.IsActive = '';
        hdNew.XuatKhoAll = false;
        hdNew.DuyetBaoGia = false;
        hdNew.DuyetBaoHanh = false;
        hdNew.BangGiaWasChanged = false;

        hdNew.MaPhieuTiepNhan = '';
        hdNew.BienSo = '';
        hdNew.SoDuDatCoc = 0;
        // Tra Nhanh
        if (hdNew.ID_HoaDon === null) {
            hdNew.MaHoaDonTraHang = '';
            hdNew.TongGiaGocHangTra = hdNew.PhaiThanhToan;
            hdNew.TongTienTra = hdNew.PhaiThanhToan - hdNew.TongChiPhi;
            hdNew.PTGiamDB = 0;
            hdNew.TongGiamGiaDB = 0;
            hdNew.TongChiPhiHangTra = 0;
            hdNew.PhaiThanhToanDB = 0;
            hdNew.DaThanhToan = self.IsGara() ? 0 : hdNew.PhaiThanhToan;
            hdNew.DaTraKhach = self.IsGara() ? 0 : hdNew.PhaiThanhToan;
            hdNew.MaHoaDonTH_NVien = 'Trả hàng ';
            hdNew.PTThueDB = 0;
            hdNew.TongThueDB = 0;

            // order by SoThuTu ASC --> group Hang Hoa by LoHang
            var arrCTsort = self.BH_HoaDonChiTiets().sort(function (a, b) {
                var x = a.SoThuTu,
                    y = b.SoThuTu;
                return x < y ? -1 : x > y ? 1 : 0;
            });
            var arrIDQuiDoi = [];
            var cthdLoHang = [];
            for (let i = 0; i < arrCTsort.length; i++) {
                let ctNew = $.extend({}, arrCTsort[i]);
                ctNew = SetDefaultPropertiesCTHD(arrCTsort[i], hdNew.MaHoaDon, hdNew.LoaiHoaDon);
                ctNew.SoLuongDaMua = 0;
                ctNew.DVTinhGiam = '%'; // default DVTGiam ='%'
                ctNew.GiaBan = ctNew.DonGia - ctNew.TienChietKhau;// get thue from DB
                if (arrCTsort[i].TienChietKhau > 0) {
                    if (arrCTsort[i].PTChietKhau === 0) {
                        ctNew.DVTinhGiam = 'VND';
                    }
                }
                ctNew.CssWarning = true; // default TraHang = false
                ctNew.GhiChu_NVThucHien = '';
                ctNew.BH_NhanVienThucHien = [];
                ctNew.GhiChu_NVTuVan = '';
                ctNew.GhiChu_NVThucHienPrint = '';
                ctNew.GhiChu_NVTuVanPrint = '';
                ctNew.ThanhPhan_DinhLuong = [];
                ctNew.ThanhPhanComBo = [];
                ctNew.ChatLieu = '1';
                ctNew.ID_ChiTietGoiDV = null;
                ctNew.LaPTPhiDichVu = false;
                ctNew.PhiDichVu = 0;
                ctNew.TongPhiDichVu = 0;
                ctNew.TenViTri = '';
                ctNew.ThoiGianThucHien = 0;

                // lo hang
                var quanLiTheoLo = ctNew.QuanLyTheoLoHang;
                let dateLot = GetNgaySX_NgayHH(ctNew);
                ctNew.NgaySanXuat = dateLot.NgaySanXuat;
                ctNew.NgayHetHan = dateLot.NgayHetHan;

                ctNew.DM_LoHang = [];
                ctNew.LotParent = quanLiTheoLo;

                // check exist in cthdLoHang
                if ($.inArray(arrCTsort[i].ID_DonViQuiDoi, arrIDQuiDoi) === -1) {
                    arrIDQuiDoi.unshift(arrCTsort[i].ID_DonViQuiDoi);
                    ctNew.SoThuTu = cthdLoHang.length + 1;
                    ctNew.IDRandom = CreateIDRandom('RandomCT_');
                    if (quanLiTheoLo) {
                        // push DM_Lo
                        let objLot = $.extend({}, ctNew);
                        objLot.DM_LoHang = [];
                        objLot.HangCungLoais = [];
                        ctNew.DM_LoHang.push(objLot);
                    }
                    cthdLoHang.push(arrCTsort[i]);
                }
                else {
                    // find in cthdLoHang with same ID_QuiDoi
                    for (let j = 0; j < cthdLoHang.length; j++) {
                        if (cthdLoHang[j].ID_DonViQuiDoi === ctNew.ID_DonViQuiDoi) {
                            if (quanLiTheoLo) {
                                let objLot = $.extend({}, ctNew);
                                objLot.LotParent = false;
                                objLot.IDRandom = CreateIDRandom('RandomCT_');
                                objLot.DM_LoHang = [];
                                objLot.HangCungLoais = [];
                                cthdLoHang[j].DM_LoHang.push(objLot);
                            }
                            else {
                                ctNew.IDRandom = CreateIDRandom('RandomCT_');
                                ctNew.LaConCungLoai = true;
                                cthdLoHang[j].HangCungLoais.push(arrCTsort[i]);
                            }
                            break;
                        }
                    }
                }
            }
            // sort CTHD by SoThuTu desc
            cthdLoHang = cthdLoHang.sort(function (a, b) {
                var x = a.SoThuTu, y = b.SoThuTu;
                return x < y ? 1 : x > y ? -1 : 0;
            });
            localStorage.setItem('lcCTHDTHSaoChep', JSON.stringify(cthdLoHang));
            self.gotoGara();
        }
        else {
            // Tra theo HD san co
            hdNew.MaHoaDonTraHang = hdNew.MaHoaDonGoc;
            hdNew.PhaiThanhToanDB = 0;// todo check when giamtruhd muagoc
            hdNew.TongGiaGocHangTra = 0;
            hdNew.TongChiPhiHangTra = 0;
            hdNew.TongChiPhi = 0;
            hdNew.TongTienTra = 0;
            hdNew.DaThanhToan = 0;
            hdNew.TongGiamGiaDB = 0;
            hdNew.PhaiThanhToan = 0;
            hdNew.PTGiamDB = RoundDecimal(hdNew.TongGiamGia === null ? 0 : hdNew.TongGiamGia / (item.TongTienHang - item.TongTienThue) * 100);
            hdNew.MaHoaDonTH_NVien = 'Trả hàng ' + hdNew.MaHoaDonGoc + ' - ' + hdNew.TenNhanVien;
            hdNew.PTThueDB = item.PTThueHoaDon;
            hdNew.TongThueDB = item.TongTienThue;

            const allComboHD = await vmThanhPhanCombo.GetAllCombo_byIDHoaDon(item.ID);
            ajaxHelper(BH_HoaDonUri + 'GetCTHoaDon_afterTraHang?idHoaDon=' + hdNew.ID_HoaDon, 'GET').done(function (data) {
                if (data === null || (data !== null && data.length === 0)) {
                    ShowMessage_Danger('Không còn mặt hàng để hoàn trả cho hóa đơn');
                    return;
                }
                // data contains {TyLeChuyenDoi, QuyCach, LaDonViChuan}
                if (data !== null && data.length > 0) {
                    let ctAfter = $.grep(data, function (x) {
                        return x.SoLuongConLai > 0;
                    });
                    if (ctAfter.length === 0) {
                        localStorage.removeItem('lcHDTHSaoChep');
                        ShowMessage_Danger('Không còn mặt hàng để hoàn trả cho hóa đơn');
                        return false;
                    }
                    var countThis = 0;
                    // update soluong damua to crhd
                    for (let i = 0; i < ctAfter.length; i++) {
                        for (let j = 0; j < self.BH_HoaDonChiTiets().length; j++) {
                            let itFor = self.BH_HoaDonChiTiets()[j];
                            if (itFor.ID_ChiTietGoiDV === ctAfter[i].ID) {
                                countThis += 1;
                                self.BH_HoaDonChiTiets()[j].SoLuongConLai = ctAfter[i].SoLuongConLai;
                                break;
                            }
                        }
                    }
                    if (countThis === 0) {
                        localStorage.removeItem('lcHDTHSaoChep');
                        ShowMessage_Danger('Không còn mặt hàng để hoàn trả cho hóa đơn');
                        return false;
                    }
                    // order by SoThuTu ASC --> group Hang Hoa by LoHang
                    ctAfter = self.BH_HoaDonChiTiets().sort(function (a, b) {
                        var x = a.SoThuTu,
                            y = b.SoThuTu;
                        return x < y ? -1 : x > y ? 1 : 0;
                    });

                    ctAfter = $.grep(ctAfter, function (x) {
                        return x.SoLuongConLai > 0;
                    })
                    var arrIDQuiDoi = [];
                    var cthdLoHang = [];
                    for (let i = 0; i < ctAfter.length; i++) {
                        let ctNew = $.extend({}, ctAfter[i]);
                        ctNew = SetDefaultPropertiesCTHD(ctNew, hdNew.MaHoaDon, hdNew.LoaiHoaDon);
                        ctNew.SoLuong = 0;
                        ctNew.GhiChu_NVThucHien = '';
                        ctNew.GhiChu_NVThucHienPrint = '';
                        ctNew.GhiChu_NVTuVan = '';
                        ctNew.GhiChu_NVTuVanPrint = '';
                        ctNew.SoLuongDaMua = ctNew.SoLuongConLai;
                        ctNew.DVTinhGiam = '%';
                        ctNew.GiaBan = ctNew.DonGia;// trahang: lay giaban chua tinh thue
                        ctNew.ThanhTien = 0;
                        ctNew.ThanhToan = 0;
                        if (ctNew.TienChietKhau > 0) {
                            if (ctNew.PTChietKhau === 0) {
                                ctNew.DVTinhGiam = 'VND';
                            }
                        }
                        ctNew.CssWarning = false;
                        ctNew.ChatLieu = '1';
                        if (hdNew.LoaiHoaDonGoc === 19) {
                            ctNew.ChatLieu = '2';
                        }
                        ctNew.ThanhPhan_DinhLuong = [];
                        ctNew.TongPhiDichVu = 0;
                        ctNew.LaPTPhiDichVu = false;
                        ctNew.PhiDichVu = false;
                        ctNew.TenViTri = '';
                        ctNew.ThoiGianThucHien = 0;
                        // lo hang
                        let quanLiTheoLo = ctNew.QuanLyTheoLoHang;
                        ctNew.QuanLyTheoLoHang = quanLiTheoLo;
                        ctNew.DM_LoHang = [];
                        ctNew.LotParent = quanLiTheoLo;

                        let dateLot = GetNgaySX_NgayHH(ctNew);
                        ctNew.NgaySanXuat = dateLot.NgaySanXuat;
                        ctNew.NgayHetHan = dateLot.NgayHetHan;

                        // get tpcombo
                        let combo = $.grep(allComboHD, function (x) {
                            return x.ID_ParentCombo === ctNew.ID_ParentCombo;
                        });
                        if (combo.length > 0) {
                            ctNew.ThanhPhanComBo = combo;
                            ctNew = AssignThanhPhanComBo_toCTHD(ctNew);
                            for (let k = 0; k < ctNew.ThanhPhanComBo.length; k++) {
                                let forCb = ctNew.ThanhPhanComBo[k];
                                forCb.SoLuong = 0;
                                forCb.ThanhTien = 0;
                                forCb.PTChietKhau = 0;
                                forCb.TienChietKhau = 0;
                                forCb.ThanhToan = 0;
                                forCb.SoLuongDaMua = forCb.SoLuongConLai;
                                forCb.GiaBan = forCb.DonGia - forCb.GiamGia;
                                forCb.DonGia = forCb.GiaBan;
                                forCb.ID_ChiTietGoiDV = forCb.ID;
                            }
                        }
                        else {
                            ctNew.ThanhPhanComBo = [];
                        }

                        if ($.inArray(ctNew.ID_DonViQuiDoi, arrIDQuiDoi) === -1) {
                            arrIDQuiDoi.unshift(ctNew.ID_DonViQuiDoi);
                            // push CTHD
                            ctNew.SoThuTu = cthdLoHang.length + 1;
                            ctNew.IDRandom = CreateIDRandom('RandomCT_');
                            if (quanLiTheoLo) {
                                // push DM_Lo
                                let objLot = $.extend({}, ctNew);
                                objLot.DM_LoHang = [];
                                objLot.HangCungLoais = [];
                                ctNew.DM_LoHang.push(objLot);
                            }
                            cthdLoHang.push(ctNew);
                        }
                        else {
                            // find in cthdLoHang with same ID_QuiDoi
                            for (let j = 0; j < cthdLoHang.length; j++) {
                                if (cthdLoHang[j].ID_DonViQuiDoi === ctNew.ID_DonViQuiDoi) {
                                    if (quanLiTheoLo) {
                                        // push DM_Lo
                                        let objLot = $.extend({}, ctNew);
                                        objLot.DM_LoHang = [];
                                        objLot.HangCungLoais = [];
                                        objLot.LotParent = false;
                                        objLot.IDRandom = CreateIDRandom('RandomCT_');
                                        cthdLoHang[j].DM_LoHang.push(objLot);
                                    }
                                    else {
                                        ctNew.IDRandom = CreateIDRandom('RandomCT_');
                                        ctNew.LaConCungLoai = true;
                                        cthdLoHang[j].HangCungLoais.push(ctNew);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    // sort CTHD by SoThuTu desc
                    cthdLoHang = cthdLoHang.sort(function (a, b) {
                        var x = a.SoThuTu, y = b.SoThuTu;
                        return x < y ? 1 : x > y ? -1 : 0;
                    });

                    localStorage.setItem('lcCTHDTHSaoChep', JSON.stringify(cthdLoHang));
                    self.gotoGara();
                }
            });
        }

        // HoaDon
        hdNew.TongTienThue = 0;// do tienthue nay bind at hd doitra
        hdNew.TongThueKhachHang = 0;
        hdNew.GiamTruThanhToanBaoHiem = 0;
        hdNew.HeaderBH_GiaTriPtram = 0;
        hdNew.HeaderBH_Type = 1;
        hdNew.CongThucBaoHiem = 0;
        hdNew.HoanTraThuKhac = 0;
        hdNew.TongTienHang = 0;
        hdNew.TongGiamGia = 0;
        hdNew.HoanTraTamUng = 0;
        hdNew.KhachDaTra = 0;
        hdNew.IsKhuyenMaiHD = false;
        hdNew.IsOpeningKMaiHD = false;
        hdNew.KhuyeMai_GiamGia = 0;
        hdNew.TTBangDiem = 0;
        hdNew.DiemGiaoDich = 0;
        hdNew.DiemQuyDoi = 0;
        hdNew.DiemHienTai = 0;
        hdNew.TienATM = 0;
        hdNew.DiemCong = 0; // use when KM_Cong diem
        hdNew.ID_NhomDTApplySale = null;  // apply giam gia theo nhom
        localStorage.setItem('lcHDTHSaoChep', JSON.stringify(hdNew));
    }
    function ResetPhieuThuChi() {
        self.GhiChu_PhieuThu('');
        self.TongTT_PhieuThu(0);
        self.NoSau(0);
        self.TienThua_PT(0);
        self.selectID_KhoanThu(undefined);
        self.ThuTuKhach('');// at list TraHang
        $(txtTienMat).val(0);
        $(txtTienATM).val(0);
        $(txtTienGui).val(0);
        $(txtTienTheGiaTri).val(0);
        $('#txtBillCode').val('');
    }
    self.showPopThanhToan = function (hd) {

        let item = $.extend({}, true, hd);
        item.DienThoaiBaoHiem = hd.BH_SDT;

        if (self.CongTy().length > 0) {
            vmThanhToan.inforCongTy = {
                TenCongTy: self.CongTy()[0].TenCongTy,
                DiaChiCuaHang: self.CongTy()[0].DiaChi,
                LogoCuaHang: Open24FileManager.hostUrl + self.CongTy()[0].DiaChiNganHang,
                TenChiNhanh: $('#_txtTenDonVi').text(),
            };
        }

        switch (hd.LoaiHoaDon) {
            case 1:
            case 25:
                item.PhaiThanhToan = hd.PhaiThanhToan - hd.TongTienHDTra;
                break;
            case 6:
                item.PhaiThanhToan = hd.TongTienHDTra;
                break;
            default:
                break;
        }
        vmThanhToan.showModalThanhToan(item);
    }
    $('#txtNgayTaoInput').on('apply.daterangepicker', function (ev, picker) {
        $(this).val(picker.startDate.format('DD/MM/YYYY') + ' - ' + picker.endDate.format('DD/MM/YYYY'));
        SearchHoaDon();
    });
    // ThanhToanHD: POS, ChuyenKhoan
    self.ListAccountPOS = ko.observableArray();
    self.ListAccountChuyenKhoan = ko.observableArray();
    self.filterAcCK = ko.observable();
    self.filterAcPOS = ko.observable();
    self.selectID_POS = ko.observable();
    self.selectID_ChuyenKhoan = ko.observable();
    self.KhoanThuChis = ko.observableArray();
    self.KhoanChis = ko.observableArray();
    self.AllKhoanThuChis = ko.observableArray();
    self.selectID_KhoanThu = ko.observableArray();
    self.AllAccountBank = ko.observableArray();
    const txtTienMat = '#txtTienMat';
    const txtTienATM = '#txtTienATM';
    const txtTienGui = '#txtTienGui';
    const txtTienTheGiaTri = '#txtTienTheGiaTri_PhieuThu';
    function GetDM_TaiKhoanNganHang() {
        ajaxHelper(Quy_HoaDonUri + 'GetAllTaiKhoanNganHang_ByDonVi?idDonVi=' + id_donvi, 'GET').done(function (x) {
            if (x.res === true) {
                let data = x.data;
                self.AllAccountBank(data);
                vmThanhToan.listData.AccountBanks = data;

                for (let i = 0; i < data.length; i++) {
                    if (data[i].TaiKhoanPOS === true) {
                        self.ListAccountPOS.push(data[i]);
                    }
                    else {
                        self.ListAccountChuyenKhoan.push(data[i]);
                    }
                }
            }
        })
    }
    function GetAllQuy_KhoanThuChi() {
        ajaxHelper(Quy_HoaDonUri + 'GetQuy_KhoanThuChi', 'GET').done(function (x) {
            if (x.res === true) {
                let data = x.data;
                var khoanthu = $.grep(data, function (x) {
                    return x.LaKhoanThu === true;
                });
                self.KhoanThuChis(khoanthu);
                var khoanchi = $.grep(data, function (x) {
                    return x.LaKhoanThu === false;
                });
                self.KhoanChis(khoanchi);
                vmThanhToan.listData.KhoanThuChis = data;
            }
        })
    }

    function AssignMoney_InHoaDonDebit(thuTuKhach) {
        // update TienThu for List HoaDonDebit 
        for (let i = 0; i < self.ListHDisDebit().length; i++) {
            if (thuTuKhach <= self.ListHDisDebit()[i].TienMat) {
                self.ListHDisDebit()[i].TienThu = thuTuKhach;
                $('#tienthu_' + self.ListHDisDebit()[i].ID).val(formatNumber(thuTuKhach));
                if (i === 0 && self.ListHDisDebit().length > 0) {
                    for (let j = 1; j < self.ListHDisDebit().length; j++) {
                        self.ListHDisDebit()[j].TienThu = 0;
                        $('#tienthu_' + self.ListHDisDebit()[j].ID).val("0");
                    }
                }
                break;
            }
            else {
                self.ListHDisDebit()[i].TienThu = self.ListHDisDebit()[i].TienMat;
                thuTuKhach = thuTuKhach - self.ListHDisDebit()[i].TienMat;
                $('#tienthu_' + self.ListHDisDebit()[i].ID).val(formatNumber(self.ListHDisDebit()[i].TienThu));
            }
        }
    }

    // use at HDTra
    self.CongVaoTK = ko.observable();
    self.TinhNoSau = function () {
        var noHienTai = self.NoHienTai();
        if (noHienTai === undefined) {
            noHienTai = 0;
        }
        formatNumberObj($('#txtThuTuKhach'));
        var thuTuKhach = formatNumberToFloat(self.ThuTuKhach());
        var tienThua = Math.round(thuTuKhach - noHienTai);
        self.TienThua_PT(tienThua);
        var noSau = Math.round(noHienTai - thuTuKhach);
        self.NoSau(noSau);
        AssignMoney_InHoaDonDebit(thuTuKhach);
        // update TienThu for List HoaDonDebit 
        for (let i = 0; i < self.ListHDisDebit().length; i++) {
            if (thuTuKhach <= self.ListHDisDebit()[i].TienMat) {
                self.ListHDisDebit()[i].TienThu = thuTuKhach;
                $('#tienthu_' + self.ListHDisDebit()[i].ID).val(formatNumber(thuTuKhach));
                if (i === 0 && self.ListHDisDebit().length > 0) {
                    for (let j = 1; j < self.ListHDisDebit().length; j++) {
                        self.ListHDisDebit()[j].TienThu = 0;
                        $('#tienthu_' + self.ListHDisDebit()[j].ID).val("0");
                    }
                }
                break;
            }
            else {
                self.ListHDisDebit()[i].TienThu = self.ListHDisDebit()[i].TienMat;
                thuTuKhach = thuTuKhach - self.ListHDisDebit()[i].TienMat;
                $('#tienthu_' + self.ListHDisDebit()[i].ID).val(formatNumber(self.ListHDisDebit()[i].TienThu));
            }
        }
        var tongTT = 0;
        for (let i = 0; i < self.ListHDisDebit().length; i++) {
            tongTT += self.ListHDisDebit()[i].TienThu;
        }
        self.TongTT_PhieuThu(tongTT);
    }

    self.formatDateTime = function () {
        $('.datepicker_mask').datetimepicker(
            {
                format: "d/m/Y H:i",
                mask: true,
                timepicker: false,
            });
    }

    function CheckQuyenExist(maquyen) {
        var role = $.grep(self.Quyen_NguoiDung(), function (x) {
            return x.MaQuyen === maquyen;
        });
        return role.length > 0;
    }

    self.ListTypeMauIn = ko.observableArray();
    function loadMauIn() {
        $.ajax({
            url: '/api/DanhMuc/ThietLapApi/GetListMauIn?typeChungTu=' + dathangTeamplate + '&idDonVi=' + id_donvi,
            type: 'GET',
            dataType: 'json',
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            success: function (result) {
                self.ListTypeMauIn(result);
            }
        });
    }

    async function MauInHoaDon_CheckAndBind(dataMauIn) {
        if (dataMauIn.includes('ChuXe_MST')) {
            let chuxe_MST = '';
            if (!commonStatisJs.CheckNull(self.InforHDprintf().ID_ChuXe)) {
                const chuxe = await GetInforCus(self.InforHDprintf().ID_ChuXe);
                if (!$.isEmptyObject(chuxe)) {
                    chuxe_MST = chuxe.MaSoThue;
                }
            }
            self.InforHDprintf().ChuXe_MST = chuxe_MST;
        }

        if (dataMauIn.includes('BH_NoTruoc') || dataMauIn.includes('BH_NoSau') || dataMauIn.includes('BH_MaSoThue')) {
            let baohiem = {}, bh_notruoc = 0, bh_nosau = 0, bh_masothue = '';
            if (!commonStatisJs.CheckNull(self.InforHDprintf().ID_BaoHiem)) {
                baohiem = await GetInforCus(self.InforHDprintf().ID_BaoHiem);
            }
            if (!$.isEmptyObject(baohiem)) {
                bh_masothue = baohiem.MaSoThue;
                bh_nosau = baohiem.NoHienTai;
                bh_notruoc = bh_nosau - self.InforHDprintf().PhaiThanhToanBaoHiem - self.InforHDprintf().BaoHiemDaTra;
                bh_notruoc = bh_notruoc < 0 ? 0 : bh_notruoc;
            }
            self.InforHDprintf().BH_NoTruoc = bh_notruoc;
            self.InforHDprintf().BH_NoSau = bh_nosau;
            self.InforHDprintf().BH_MaSoThue = bh_masothue;
        }

        dataMauIn = dataMauIn.concat('<script src="/Scripts/knockout-3.4.2.js"></script>');
        dataMauIn = dataMauIn.concat("<style> @media print {body {-webkit-print-color-adjust: exact;}} </style>")
        dataMauIn = dataMauIn.concat(' <script src="/Content/Framework/Moment/moment.min.js"></script>');
        dataMauIn = dataMauIn.concat("<script > var item1=" + JSON.stringify(self.CTHoaDonPrint())
            + "; var item2=" + JSON.stringify(self.CTHoaDonPrintMH())
            + "; var item3=" + JSON.stringify(self.InforHDprintf())
            + "; var item4=" + JSON.stringify(self.HangMucSuaChuas())
            + "; var item5=" + JSON.stringify(self.VatDungKemTheos())
            + "; </script>");
        dataMauIn = dataMauIn.concat(" <script type='text/javascript' src='/Scripts/Thietlap/MauInTeamplate.js'></script>");
        dataMauIn = dataMauIn.replace('{Email}', "<span data-bind=\"text: InforHDprintf().Email\"></span>");
        dataMauIn = dataMauIn.replace('{TienKhachThieu_BangChu}', "<span data-bind=\"text: InforHDprintf().TienKhachThieu_BangChu\"></span>");
        dataMauIn = dataMauIn.replace('{BH_ConThieu_BangChu}', "<span data-bind=\"text: InforHDprintf().BH_ConThieu_BangChu\"></span>");
        dataMauIn = dataMauIn.replace('{KH_DaThanhToan_BangChu}', "<span data-bind=\"text: InforHDprintf().KH_DaThanhToan_BangChu\"></span>");
        dataMauIn = dataMauIn.replace('{KH_DaThanhToan_TruCocBG_BangChu}', "<span data-bind=\"text: InforHDprintf().KH_DaThanhToan_TruCocBG_BangChu\"></span>");
        dataMauIn = dataMauIn.replace('{KH_DaThanhToan_TruCocBG}', "<span data-bind=\"text: formatNumber(InforHDprintf().KH_DaThanhToan_TruCocBG,0)\"></span>");
        dataMauIn = dataMauIn.replace('{ChuXe_MST}', "<span data-bind=\"text: InforHDprintf().ChuXe_MST\"></span>");
        PrintExtraReport(dataMauIn);
    }

    async function GetMauIn_byID(id) {
        const data = await ajaxHelper('/api/DanhMuc/ThietLapApi/GetContentFIlePrint?idMauIn=' + id, 'GET').done()
            .then(function (result) {
                return result;
            });
        return data;
    }
    self.PrinDatHang = async function (item, key) {
        var cthdFormat = await GetCTHDPrint_Format(item.ID);
        self.CTHoaDonPrint(cthdFormat);

        var itemHDFormat = await GetInforHDPrint(item.ID, false);
        self.InforHDprintf(itemHDFormat);

        const dataMauIn = await GetMauIn_byID(key);
        MauInHoaDon_CheckAndBind(dataMauIn);
        //self.PrintMany(key);
    }
    self.Print_ListTempDoiTra = async function (item, key) {
        var cthdTraHang = await GetCTHDPrint_Format(item.ID_HoaDon);
        self.CTHoaDonPrint(cthdTraHang);

        var cthdDoiTra = await GetCTHDPrint_Format(item.ID);
        for (let i = 0; i < cthdDoiTra.length; i++) {
            let nvth = '';
            let nvtv = '';
            if (!commonStatisJs.CheckNull(cthdDoiTra[i].BH_NhanVienThucHien)) {
                for (let j = 0; j < cthdDoiTra[i].BH_NhanVienThucHien.length; j++) {
                    let nvien = cthdDoiTra[i].BH_NhanVienThucHien[j];
                    switch (nvien.ThucHien_TuVan) {
                        case true:
                            nvth += nvien.TenNhanVien + ', ';
                            break;
                        case false:
                            nvtv += nvien.TenNhanVien + ', ';
                            break;
                    }
                }
            }
            cthdDoiTra[i].GhiChu_NVThucHien = Remove_LastComma(nvth);
            cthdDoiTra[i].GhiChu_NVTuVan = Remove_LastComma(nvtv);
            cthdDoiTra[i].GhiChu_NVThucHienPrint = Remove_LastComma(nvth);
            cthdDoiTra[i].GhiChu_NVTuVanPrint = Remove_LastComma(nvtv);
        }

        self.CTHoaDonPrintMH(cthdDoiTra);

        let tongTienHDTra = 0, phiTraHang = 0, phaiTraKhach = 0;
        for (let i = 0; i < self.HoaDons().length; i++) {
            let itFor = self.HoaDons()[i];
            if (itFor.ID === item.ID_HoaDon) {
                tongTienHDTra = itFor.TongTienHang + itFor.TongTienThue - itFor.TongGiamGia;
                phiTraHang = itFor.TongChiPhi;
                break;
            }
        }
        var phaiTT = formatNumberToFloat(item.PhaiThanhToan) - tongTienHDTra;
        if (phaiTT > 0) {
            phaiTraKhach = 0;
        }
        else {
            phaiTraKhach = Math.abs(phaiTT);
            phaiTT = 0;
        }
        var itemHDFormat = await GetInforHDPrint(item.ID, true);
        itemHDFormat.TongTienTraHang = tongTienHDTra;
        itemHDFormat.PhiTraHang = phiTraHang;
        itemHDFormat.PhaiTraKhach = phaiTraKhach;
        itemHDFormat.TongCong = phaiTT;// ~ khach phai tra
        itemHDFormat.PhaiThanhToan = phaiTT;// ~ khach phai tra
        self.InforHDprintf(itemHDFormat);

        $.ajax({
            url: '/api/DanhMuc/ThietLapApi/GetContentFIlePrint?idMauIn=' + key,
            type: 'GET',
            dataType: 'json',
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            success: function (result) {
                let data = result;
                data = data.concat('<script src="/Scripts/knockout-3.4.2.js"></script>');
                data = data.concat(' <script src="/Content/Framework/Moment/moment.min.js"></script>');
                data = data.concat("<script > var item1=" + JSON.stringify(self.CTHoaDonPrint())
                    + "; var item4=[], item5=[]; var item2=" + JSON.stringify(self.CTHoaDonPrintMH())
                    + ";var item3=" + JSON.stringify(self.InforHDprintf()) + "; </script>");
                data = data.concat(" <script type='text/javascript' src='/Scripts/Thietlap/MauInTeamplate.js'></script>"); // MauInTeamplate.js: used to bind data in knockout
                PrintExtraReport(data); // assign content HTML into frame
            }
        });
    }
    // check format date
    const isCorrectFormat = (dateString, format) => {
        return moment(dateString, format, true).isValid()
    }
    // auto update NhomKhach(TraHang --> nâng nhóm , HuyHD --> hạ nhóm)
    self.DM_NhomDoiTuong_ChiTiets = ko.observableArray();
    function GetDM_NhomDoiTuong_ChiTiets() {
        ajaxHelper('/api/DanhMuc/DM_NhomDoiTuongAPI/' + 'GetDM_NhomDoiTuong_ChiTiets?idDonVi=' + id_donvi, 'GET').done(function (x) {
            let data = x.data;
            if (data.length > 0) {
                self.DM_NhomDoiTuong_ChiTiets(data);
            }
        });
    }

    async function GetInforCus(id) {
        if (!commonStatisJs.CheckNull(id) && id !== const_GuidEmpty) {
            var date = moment(new Date()).format('YYYY-MM-DD HH:mm');
            var xx = await ajaxHelper(DMDoiTuongUri + "GetInforKhachHang_ByID?idDoiTuong=" + id + '&idChiNhanh=' + VHeader.IdDonVi
                + '&timeStart=' + date + '&timeEnd=' + date + '&wasChotSo=false', 'GET').done(function () {
                })
                .then(function (data) {
                    if (data !== null && data.length > 0) {
                        return data[0];
                    }
                    return {};
                })
            return xx;
        }
        return {};
    }

    function Insert_ManyNhom(lstNhom) {
        var myData = {};
        myData.lstDM_DoiTuong_Nhom = lstNhom;
        $.ajax({
            data: myData,
            url: DMDoiTuongUri + "PostDM_DoiTuong_Nhom",
            type: 'POST',
            async: false,
            dataType: 'json',
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            success: function (item) {
            },
            error: function (jqXHR, textStatus, errorThrown) {
                ShowMessage_Danger('Thêm mới nhóm khách hàng thất bại');
            },
        })
    }
    // search and paging CTHD
    self.PageResult_CTHoaDons = ko.computed(function (item) {
        // filter
        var filter = self.filterHangHoa_ChiTietHD();
        var arrFilter = ko.utils.arrayFilter(self.BH_HoaDonChiTiets(), function (prod) {
            var chon = true;
            var ipLodau = locdau(filter);
            var maHH = locdau(prod.MaHangHoa);
            var tenHH = locdau(prod.TenHangHoa);
            var maLoHang = locdau(prod.MaLoHang);
            var kitudau = GetChartStart(tenHH);
            if (chon && filter) {
                chon = maHH.indexOf(ipLodau) > -1 || tenHH.indexOf(ipLodau) > -1
                    || maLoHang.indexOf(ipLodau) > -1 || kitudau.indexOf(ipLodau) > -1
                    ;
            }
            return chon;
        });
        var lenData = arrFilter.length;
        self.PageCount_CTHD(Math.ceil(lenData / self.PageSize_CTHD()));
        self.TotalRecord_CTHD(lenData);
        // paging
        var first = self.currentPage_CTHD() * self.PageSize_CTHD();
        if (arrFilter !== null) {
            return arrFilter.slice(first, first + self.PageSize_CTHD());
        }
    })
    self.PageList_CTHD = ko.computed(function () {
        var arrPage = [];
        var allPage = self.PageCount_CTHD();
        var currentPage = self.currentPage_CTHD();
        if (allPage > 4) {
            var i = 0;
            if (currentPage === 0) {
                i = parseInt(self.currentPage_CTHD()) + 1;
            }
            else {
                i = self.currentPage_CTHD();
            }
            if (allPage >= 5 && currentPage > allPage - 5) {
                if (currentPage >= allPage - 2) {
                    // get 5 trang cuoi cung
                    for (let i = allPage - 5; i < allPage; i++) {
                        let obj = {
                            pageNumber: i + 1,
                        };
                        arrPage.push(obj);
                    }
                }
                else {
                    if (currentPage === 1) {
                        for (let j = currentPage - 1; (j <= currentPage + 3) && j < allPage; j++) {
                            let obj = {
                                pageNumber: j + 1,
                            };
                            arrPage.push(obj);
                        }
                    }
                    else {
                        // get currentPage - 2 , currentPage, currentPage + 2
                        for (let j = currentPage - 2; (j <= currentPage + 2) && j < allPage; j++) {
                            let obj = {
                                pageNumber: j + 1,
                            };
                            arrPage.push(obj);
                        }
                    }
                }
            }
            else {
                // get 5 trang dau
                if (i >= 2) {
                    while (arrPage.length < 5) {
                        let obj = {
                            pageNumber: i - 1,
                        };
                        arrPage.push(obj);
                        i = i + 1;
                    }
                }
                else {
                    while (arrPage.length < 5) {
                        let obj = {
                            pageNumber: i,
                        };
                        arrPage.push(obj);
                        i = i + 1;
                    }
                }
            }
        }
        else {
            // neu chi co 1 trang --> khong hien thi DS trang
            if (allPage > 1) {
                for (let i = 0; i < allPage; i++) {
                    let obj = {
                        pageNumber: i + 1,
                    };
                    arrPage.push(obj);
                }
            }
        }
        if (self.PageResult_CTHoaDons().length > 0) {
            self.fromitem_CTHD((self.currentPage_CTHD() * self.PageSize_CTHD()) + 1);
            if (((self.currentPage_CTHD() + 1) * self.PageSize_CTHD()) > self.PageResult_CTHoaDons().length) {
                var ss = (self.currentPage_CTHD() + 1) * self.PageSize_CTHD();
                var fromItem = (self.currentPage_CTHD() + 1) * self.PageSize_CTHD();
                if (fromItem < self.TotalRecord_CTHD()) {
                    self.toitem_CTHD((self.currentPage_CTHD() + 1) * self.PageSize_CTHD());
                }
                else {
                    self.toitem_CTHD(self.TotalRecord_CTHD());
                }
            } else {
                self.toitem_CTHD((self.currentPage_CTHD() * self.PageSize_CTHD()) + self.PageSize_CTHD());
            }
        }
        return arrPage;
    });
    self.VisibleStartPage_CTHD = ko.computed(function () {
        if (self.PageList_CTHD().length > 0) {
            return self.PageList_CTHD()[0].pageNumber !== 1;
        }
    });
    self.VisibleEndPage_CTHD = ko.computed(function () {
        if (self.PageList_CTHD().length > 0) {
            return self.PageList_CTHD()[self.PageList_CTHD().length - 1].pageNumber !== self.PageCount_CTHD();
        }
    })
    self.ResetCurrentPage_CTHD = function () {
        self.currentPage_CTHD(0);
    };
    self.GoToPage_CTHD = function (page) {
        self.currentPage_CTHD(page.pageNumber - 1);
    };
    self.GetClass_CTHD = function (page) {
        return ((page.pageNumber - 1) === self.currentPage_CTHD()) ? "click" : "";
    };
    self.StartPage_CTHD = function () {
        self.currentPage_CTHD(0);
    }
    self.BackPage_CTHD = function () {
        if (self.currentPage_CTHD() > 1) {
            self.currentPage_CTHD(self.currentPage_CTHD() - 1);
        }
    }
    self.GoToNextPage_CTHD = function () {
        if (self.currentPage_CTHD() < self.PageCount_CTHD() - 1) {
            self.currentPage_CTHD(self.currentPage_CTHD() + 1);
        }
    }
    self.EndPage_CTHD = function () {
        if (self.currentPage_CTHD() < self.PageCount_CTHD() - 1) {
            self.currentPage_CTHD(self.PageCount_CTHD() - 1);
        }
    }
    self.DM_MauIn = ko.observableArray();
    function GetAllMauIn_byChiNhanh() {
        ajaxHelper('/api/DanhMuc/ThietLapApi/GetAllMauIn_ByChiNhanh?idChiNhanh=' + id_donvi, 'GET').done(function (data) {
            if (data !== null) {
                self.DM_MauIn(data);
            }
        });
    }
    self.DMKhuyenMai = ko.observableArray();
    function GetKM_CTKhuyenMai() {
        ajaxHelper('/api/DanhMuc/BH_KhuyenMaiAPI/' + 'GetKM_CTKhuyenMai?idDonVi=' + id_donvi, 'GET').done(function (data) {
            if (data !== null) {
                self.DMKhuyenMai(data);
                GetList_KMApDung();
            }
        });
    }
    function Check_KhuyenMai_Active(idKhuyenMai) {
        var itemKM = $.grep(self.DMKhuyenMai(), function (x) {
            return x.ID_KhuyenMai === idKhuyenMai;
        });
        if (itemKM.length > 0) {
            // check Han su dung
            var now = moment(new Date()).format('YYYY-MM-DD');
            var ngayHetHan = moment(itemKM[0].ThoiGianKetThuc).format('YYYY-MM-DD');
            if (ngayHetHan >= now) {
                return true;
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }
    }
    // saochep/trahang co KM
    self.KM_KMApDung = ko.observableArray();
    self.NhomHangHoas = ko.observableArray();
    function GetList_KMApDung() {
        for (let i = 0; i < self.DMKhuyenMai().length; i++) {
            var itemKM = self.DMKhuyenMai()[i];
            var objKhuyenMai = {
                ID_KhuyenMai: null,
                TenKhuyenMai: "",
                HinhThuc: "", // 21: Mua hang giam hang; 22: mua hang tang hang; 23: mua hang tang diem; 24: gia ban theo SL Mua
                HinhThucKhuyenMai: "",
                Months: [],
                Dates: [],
                Hours: [],
                Days: [],
                ApDungNgaySinh: 0,
                ID_QuyDoiMuas: [],
                ID_QuyDoiTangs: [],
                ID_NhomHHMuas: [],
                ID_NhomHHTangs: [],
                ID_NhomKHs: [],
                ID_NhanViens: [],
                DM_KhuyenMai_ChiTiet: [],
                Note_HinhThuc: '', // ghi chu tuong ung voi tung loai hinh thuc KM
            };
            objKhuyenMai.ID_KhuyenMai = itemKM.ID;
            objKhuyenMai.TenKhuyenMai = itemKM.TenKhuyenMai;
            objKhuyenMai.HinhThucKhuyenMai = itemKM.TenHinhThucKM;
            objKhuyenMai.HinhThuc = itemKM.HinhThuc;
            objKhuyenMai.ApDungNgaySinh = itemKM.ApDungNgaySinhNhat;
            if (itemKM.ThangApDung !== '') {
                objKhuyenMai.Months = itemKM.ThangApDung.split('_');
            }
            if (itemKM.NgayApDung !== '') {
                objKhuyenMai.Dates = itemKM.NgayApDung.split('_');
            }
            if (itemKM.ThuApDung !== '') {
                objKhuyenMai.Days = itemKM.ThuApDung.split('_');
            }
            if (itemKM.GioApDung !== '') {
                objKhuyenMai.Hours = itemKM.GioApDung.split('_');
            }
            for (let j = 0; j < itemKM.DM_KhuyenMai_ApDung.length; j++) {
                var itemKM_AD = itemKM.DM_KhuyenMai_ApDung[j];
                if (itemKM_AD.ID_NhomKhachHang !== null && $.inArray(itemKM_AD.ID_NhomKhachHang, objKhuyenMai.ID_NhomKHs) === -1) {
                    objKhuyenMai.ID_NhomKHs.push(itemKM_AD.ID_NhomKhachHang);
                }
                if (itemKM_AD.ID_NhanVien !== null && $.inArray(itemKM_AD.ID_NhanVien, objKhuyenMai.ID_NhanViens) === -1) {
                    objKhuyenMai.ID_NhanViens.push(itemKM_AD.ID_NhanVien);
                }
            }
            for (let j = 0; j < itemKM.DM_KhuyenMai_ChiTiet.length; j++) {
                var itemKMCT = itemKM.DM_KhuyenMai_ChiTiet[j];
                if (itemKMCT.ID_DonViQuiDoiMua !== null && $.inArray(itemKMCT.ID_DonViQuiDoiMua, objKhuyenMai.ID_QuyDoiMuas) === -1) {
                    objKhuyenMai.ID_QuyDoiMuas.push(itemKMCT.ID_DonViQuiDoiMua);
                }
                if (itemKMCT.ID_DonViQuiDoi !== null && $.inArray(itemKMCT.ID_DonViQuiDoi, objKhuyenMai.ID_QuyDoiTangs) === -1) {
                    objKhuyenMai.ID_QuyDoiTangs.push(itemKMCT.ID_DonViQuiDoi);
                }
                if (itemKMCT.ID_NhomHangHoa !== null) {
                    var arrNhomChildTang = GetAll_IDNhomChild_ofNhomHH([itemKMCT.ID_NhomHangHoa]);
                    if (arrNhomChildTang.length > 0) {
                        for (let k = 0; k < arrNhomChildTang.length; k++) {
                            if ($.inArray(arrNhomChildTang[k], objKhuyenMai.ID_NhomHHTangs) === -1) {
                                objKhuyenMai.ID_NhomHHTangs.push(arrNhomChildTang[k]);
                            }
                        }
                    }
                }
                if (itemKMCT.ID_NhomHangHoaMua !== null) {
                    var arrNhomChildMua = GetAll_IDNhomChild_ofNhomHH([itemKMCT.ID_NhomHangHoaMua]);
                    if (arrNhomChildMua.length > 0) {
                        for (let k = 0; k < arrNhomChildMua.length; k++) {
                            if ($.inArray(arrNhomChildMua[k], objKhuyenMai.ID_NhomHHMuas) === -1) {
                                objKhuyenMai.ID_NhomHHMuas.push(arrNhomChildMua[k]);
                            }
                        }
                    }
                }
            }
            // sort to do check SoLuongMua tương ứng với GiaKhuyenMai
            itemKM.DM_KhuyenMai_ChiTiet = itemKM.DM_KhuyenMai_ChiTiet.sort(function (a, b) {
                var x = a.SoLuongMua,
                    y = b.SoLuongMua;
                return x < y ? -1 : x > y ? 1 : 0;
            });
            objKhuyenMai.DM_KhuyenMai_ChiTiet = itemKM.DM_KhuyenMai_ChiTiet;
            self.KM_KMApDung.push(objKhuyenMai);
        }
        // remove KMai tang diem if HeThong khong cai dat tinh nang tich diem
        if (self.ThietLap().TinhNangTichDiem === false) {
            var listKM = $.grep(self.KM_KMApDung(), function (x) {
                return x.HinhThuc !== 14 && x.HinhThuc !== 23;
            });
            self.KM_KMApDung(listKM);
        }
    }
    function CheckKM_IsApDung(idNhanVien, itemHD) {
        var isApDung = false;
        var dtNow = new Date();
        var _month = (dtNow.getMonth() + 1).toString(); // 1-12 (+1 because getMoth() return 0-11)
        var _date = (dtNow.getDate()).toString(); // 1- 31
        var _hours = (dtNow.getHours()).toString(); // 1-24
        var _day = (dtNow.getDay() + 1).toString(); // mon:2, tues:3, wed:4, thur:5, fri:6, sat: 7, sun: 8
        var _weekofMonth = Math.ceil(dtNow.getDate() / 7); // get week of Month ( 1- 5);
        var idNhomKH = itemHD.IDNhomDoiTuongs;
        var ngaysinhFull = itemHD.NgaySinh_NgayTLap;
        var ngaysinh = 0;
        var thangsinh = 0;
        var tuansinh = 0;
        if (ngaysinhFull !== 0 && ngaysinhFull !== null) {
            var dtNgaySinh = new Date(moment(ngaysinhFull).format('YYYY-MM-DD')); // must format 'YYYY-MM-DD'
            // get day, moth from dayFull
            ngaysinh = dtNgaySinh.getDate();
            thangsinh = (dtNgaySinh.getMonth() + 1).toString();
            tuansinh = Math.ceil(dtNgaySinh.getDate() / 7);
        }
        // get list KM by ID_DoiTuong and ID_NhanVien
        var arrKM_forDT = [];
        for (let i = 0; i < self.KM_KMApDung().length; i++) {
            // get KM apply for HangHoa
            var xItem = self.KM_KMApDung()[i];
            if (xItem.ApDungNgaySinh !== 0) {
                // if chose KH
                if (!commonStatisJs.CheckNull(itemHD.ID_DoiTuong) && itemHD.ID_DoiTuong !== const_GuidEmpty) {
                    // check ID_NhanVien, ID_nhomKH
                    if ((xItem.Months.length === 0 || $.inArray(_month, xItem.Months) > -1)
                        && (xItem.Dates.length === 0 || $.inArray(_date, xItem.Dates) > -1)
                        && (xItem.Days.length === 0 || $.inArray(_day, xItem.Days) > -1)
                        && (xItem.Hours.length === 0 || $.inArray(_hours, xItem.Hours) > -1)
                        && (xItem.ID_NhanViens.length === 0 || $.inArray(idNhanVien, xItem.ID_NhanViens) > -1)
                        && (xItem.ID_NhomKHs.length === 0 || $.inArray(idNhomKH, xItem.ID_NhomKHs) > -1)) {
                        switch (xItem.ApDungNgaySinh) {
                            case 1: // ap dung ngay sinh theo ngay
                                if (ngaysinh.toString() === _date) {
                                    isApDung = true;
                                }
                                break;
                            case 2: // ap dung ngay sinh theo tuan
                                if (tuansinh.toString() === _weekofMonth) {
                                    isApDung = true;
                                }
                                break;
                            case 3: // ap dung ngay sinh theo thang
                                if (thangsinh.toString() === _month) {
                                    isApDung = true;// esc for KM_KMApDung
                                }
                                break;
                        }
                    }
                }
            }
            else {
                // if ApDungNgaySinh = 0
                if ((xItem.Months.length === 0 || $.inArray(_month, xItem.Months) > -1)
                    && (xItem.Dates.length === 0 || $.inArray(_date, xItem.Dates) > -1)
                    && (xItem.Days.length === 0 || $.inArray(_day, xItem.Days) > -1)
                    && (xItem.Hours.length === 0 || $.inArray(_hours, xItem.Hours) > -1)
                    && (xItem.ID_NhanViens.length === 0 || $.inArray(idNhanVien, xItem.ID_NhanViens) > -1)
                    && (xItem.ID_NhomKHs.length === 0 || $.inArray(idNhomKH, xItem.ID_NhomKHs) > -1)) {
                    isApDung = true;
                }
            }
        }
        return isApDung;
    }
    function GetAllNhomHangHoas() {
        ajaxHelper('/api/DanhMuc/DM_NhomHangHoaAPI/' + 'GetDM_NhomHangHoa', 'GET').done(function (data) {
            if (data !== null) {
                for (let i = 0; i < data.length; i++) {
                    if (data[i].ID_Parent === null) {
                        var objParent = {
                            ID: data[i].ID,
                            TenNhomHangHoa: data[i].TenNhomHangHoa,
                            Childs: [],
                        }
                        for (let j = 0; j < data.length; j++) {
                            if (data[j].ID !== data[i].ID && data[j].ID_Parent === data[i].ID) {
                                var objChild =
                                {
                                    ID: data[j].ID,
                                    TenNhomHangHoa: data[j].TenNhomHangHoa,
                                    ID_Parent: data[i].ID,
                                    Child2s: []
                                };
                                for (let k = 0; k < data.length; k++) {
                                    if (data[k].ID_Parent !== null && data[k].ID_Parent === data[j].ID) {
                                        var objChild2 =
                                        {
                                            ID: data[k].ID,
                                            TenNhomHangHoa: data[k].TenNhomHangHoa,
                                            ID_Parent: data[j].ID,
                                        };
                                        objChild.Child2s.push(objChild2);
                                    }
                                }
                                objParent.Childs.push(objChild);
                            }
                        }
                        self.NhomHangHoas.push(objParent);
                    }
                }
            }
        });
    };
    function GetAll_IDNhomChild_ofNhomHH(arrIDNhomHang) {
        var arrNhomHHChilds = [];
        // get all IDChild of ID_Parent
        var lc_NhomHangHoas = self.NhomHangHoas();
        for (let j = 0; j < lc_NhomHangHoas.length; j++) {
            if (lc_NhomHangHoas[j].Childs.length > 0 && $.inArray(lc_NhomHangHoas[j].Childs[0].ID_Parent, arrIDNhomHang) > -1) {
                for (let k = 0; k < lc_NhomHangHoas[j].Childs.length; k++) {
                    arrNhomHHChilds.push(lc_NhomHangHoas[j].Childs[k].ID);
                    if (lc_NhomHangHoas[j].Childs[k].Child2s.length > 0) {
                        for (let i = 0; i < lc_NhomHangHoas[j].Childs[k].Child2s.length; i++) {
                            arrNhomHHChilds.push(lc_NhomHangHoas[j].Childs[k].Child2s[i].ID);
                        }
                    }
                }
            }
        }
        // add ID_Parent into arrNhomHHChilds
        for (let i = 0; i < arrIDNhomHang.length; i++) {
            arrNhomHHChilds.push(arrIDNhomHang[i]);
        }
        return arrNhomHHChilds;
    }

    async function CheckHoaDon_DaXuLy(idHoaDon, loaiCheck) {
        let xx = await $.getJSON('/api/DanhMuc/GaraAPI/' + 'CheckHoaDon_DaXuLy?idHoaDon=' + idHoaDon + '&loaiHoaDon=' + loaiCheck).done(function () {
        }).then(function (x) {
            return x;
        })
        return xx;
    }

    async function CheckPTN_DaBiHuy(idPTN) {
        if (commonStatisJs.CheckNull(idPTN) || idPTN === const_GuidEmpty) {
            return false;
        }
        let xx = await $.getJSON('/api/DanhMuc/GaraAPI/' + 'CheckPTN_DaBiHuy?idPhieuTiepNhan=' + idPTN).done(function () {
        }).then(function (x) {
            return x;
        })
        return xx;
    }
    async function CheckPTN_isDeleted(idPTN) {
        if (commonStatisJs.CheckNull(idPTN) || idPTN === const_GuidEmpty) {
            return false;
        }
        let xx = await $.getJSON('/api/DanhMuc/GaraAPI/' + 'CheckPTN_isDeleted?idPhieuTiepNhan=' + idPTN).done(function () {
        }).then(function (x) {
            return x;
        })
        return xx;
    }
    async function CheckHoaDon_isDeleted(idHoaDon) {
        if (commonStatisJs.CheckNull(idHoaDon) || idHoaDon === const_GuidEmpty) {
            return false;
        }
        let xx = await $.getJSON(BH_HoaDonUri + 'CheckHoaDon_isDeleted?idHoaDon=' + idHoaDon).done(function () {
        }).then(function (x) {
            return x;
        })
        return xx;
    }

    async function RestoreInvoice_fromDB(idHoaDon) {
        if (commonStatisJs.CheckNull(idHoaDon) || idHoaDon === const_GuidEmpty) {
            return false;
        }
        let xx = await $.getJSON(BH_HoaDonUri + 'RestoreInvoice?idHoaDon=' + idHoaDon).done(function () {
        }).then(function (x) {
            return x;
        })
        return xx;
    }

    async function CheckXuLyHet_DonDatHang_AndUpdateTrangThaiBG(loaiHoaDon, idHoaDon, idDatHang) {
        // idHoaDon: không sử dụng (chỉ truyền vào thôi, vì func cũ đang dùng)
        if ((loaiHoaDon === 1 || loaiHoaDon === 25) && idDatHang !== null && idDatHang !== const_GuidEmpty) {
            const xx = await ajaxHelper(BH_HoaDonUri + 'CheckXuLyHet_DonDathang?idHoaDon=' + idHoaDon + '&idDatHang=' + idDatHang, 'GET').done()
                .then(function (data) {
                    return data;
                });

            const trangThaiBG = xx ? 3 : 2;

            const dataUpdate = await ajaxHelper(BH_HoaDonUri + 'Update_StatusHD?id=' + idDatHang + '&Status=' + trangThaiBG, 'POST').done()
                .then(function (data) {
                    return data;
                });
        }
    }

    self.RestoreInvoice = async function (item) {
        const maHoaDon = item.MaHoaDon;
        const loaiHoaDon = item.LoaiHoaDon;
        let sLoai = '', sLoaiLowercase = '';

        switch (loaiHoaDon) {
            case 1:
            case 25:
                {
                    sLoai = 'Hóa đơn';
                    sLoaiLowercase = 'hóa đơn';
                }
                break;
            case 3:
                {
                    sLoai = 'Báo giá';
                    sLoaiLowercase = 'báo giá';
                }
                break;
        }

        if (loaiHoaDon === 25) {
            const checkPTN = await CheckPTN_isDeleted(item.ID_PhieuTiepNhan);
            if (checkPTN) {
                ShowMessage_Danger(sLoai + ' thuộc phiếu tiếp nhận đã bị hủy. Vui lòng khôi phục lại Phiếu tiếp nhận');
                return;
            }
        }

        const checkHD = await CheckHoaDon_isDeleted(item.ID_HoaDon, 3);
        if (checkHD) {
            ShowMessage_Danger(sLoai + ' thuộc báo giá đã bị hủy. Vui lòng khôi phục lại Báo giá');
            return;
        }

        dialogConfirm('Khôi phục ' + sLoaiLowercase, 'Bạn có chắc chắn muốn khôi phục ' + sLoaiLowercase + ' <b>' + maHoaDon + '</b> không?', async function () {
            const dataReturn = await RestoreInvoice_fromDB(item.ID);
            if (dataReturn) {
                await CheckXuLyHet_DonDatHang_AndUpdateTrangThaiBG(loaiHoaDon, item.ID_HoaDon, item.ID_HoaDon);
                ShowMessage_Success('Khôi phục ' + sLoaiLowercase + ' thàng công');


                let diary = {
                    ID_DonVi: VHeader.IdDonVi,
                    ID_NhanVien: VHeader.IdNhanVien,
                    ChucNang: "Khôi phục " + sLoaiLowercase,
                    NoiDung: "Khôi phục ".concat(sLoaiLowercase, ' ', item.MaHoaDon),
                    NoiDungChiTiet: "Khôi phục ".concat(sLoaiLowercase, ': ', item.MaHoaDon,
                        '<br /> Người khôi phục: ', VHeader.UserLogin),
                    LoaiNhatKy: 2
                }
                Insert_NhatKyThaoTac_1Param(diary);

                SearchHoaDon();
            }
        })
    }

    self.CheckHD_DaXuatKho = async function (item, type) {
        if (type === 2) {
            let loaiCheck = 6;
            switch (item.LoaiHoaDon) {
                case 1:
                    loaiCheck = 6;
                    break;
                case 3:
                    loaiCheck = 25;
                    break;
                case 25:
                    loaiCheck = 8;
                    break;
            }

            let check = await CheckHoaDon_DaXuLy(item.ID, loaiCheck);
            let mes = '';
            if (check) {
                switch (item.LoaiHoaDon) {
                    case 1:
                        mes = 'Hóa đơn đã được trả hàng. Không thể sửa đổi';
                        break;
                    case 3:
                        mes = 'Báo giá đã tạo hóa đơn. Không thể sửa đổi';
                        break;
                    case 25:
                        mes = 'Hóa đơn đã xuất kho. Không thể sửa đổi';
                        break;
                }
            }

            if (!commonStatisJs.CheckNull(mes)) {
                commonStatisJs.ShowMessageDanger(mes);
                return;
            }

            if (item.LoaiHoaDon === 25) {
                check = await CheckHoaDon_DaXuLy(item.ID, 6);
                if (check) {
                    commonStatisJs.ShowMessageDanger('Hóa đơn đã được trả hàng. Không thể sửa đổi');
                    return;
                }
            }

            modelGiaoDich.SaoChepHD_KhuyenMai(item, type);
        }
        else {
            self.SaoChepHD_KhuyenMai(item, type);
        }
    }

    self.UpdateThongTinBaoHiem = function (item) {
        vmThongTinThanhToanBaoHiem.inforLogin = {
            ID_DonVi: VHeader.IdDonVi,
            ID_NhanVien: VHeader.IdNhanVien,
            UserLogin: VHeader.UserLogin,
        };
        vmThongTinThanhToanBaoHiem.showModaUpdate(item);
    }

    function GetPTChietKhauHang_HeaderBH() {
        var ptCKHang = 0;
        var arrCTsort = self.BH_HoaDonChiTiets();
        var arr = $.grep(arrCTsort, function (x) {
            return x.PTChietKhau === arrCTsort[0].PTChietKhau;
        });
        if (arr.length === arrCTsort.length) {
            ptCKHang = arrCTsort[0].PTChietKhau;
        }

        var gtri = 0;
        var arrDonGiaBH = $.grep(arrCTsort, function (x) {
            return x.DonGiaBaoHiem > 0;
        });
        if (arrDonGiaBH.length === arrCTsort.length) {
            gtri = RoundDecimal(arrDonGiaBH[0].DonGiaBaoHiem / arrDonGiaBH[0].GiaBan * 100, 3);
        }
        return {
            PTChietKhauHH: ptCKHang,
            HeaderBH: gtri,
        }
    }

    function GetNgaySX_NgayHH(ctDoing) {
        var ngaysx = ctDoing.NgaySanXuat;
        if (!commonStatisJs.CheckNull(ngaysx)) {
            ngaysx = moment(ngaysx).format('DD/MM/YYYY');
        }
        var hansd = ctDoing.NgayHetHan;
        if (!commonStatisJs.CheckNull(hansd)) {
            hansd = moment(hansd).format('DD/MM/YYYY');
        }
        return {
            NgaySanXuat: ngaysx,
            NgayHetHan: hansd,
        }
    }

    async function GetInforHD_fromDB(id) {
        if (!commonStatisJs.CheckNull(id) && id !== const_GuidEmpty) {
            let xx = await ajaxHelper(BH_HoaDonUri + "Get_InforHoaDon_byID?id=" + id + '&getCTHD=false', 'GET').done()
                .then(function (data) {
                    if (data !== null) {
                        return data;
                    }
                    return {};
                })
            return xx;
        }
        return {};
    }

    async function GetChietKhauNV_byIDHoaDon(id) {
        if (!commonStatisJs.CheckNull(id) && id !== const_GuidEmpty) {
            let xx = ajaxHelper(BH_HoaDonUri + 'GetChietKhauNV_byIDHoaDon?idHoaDon=' + id, 'GET').done()
                .then(function (x) {
                    if (x.res) {
                        return x.data;
                    }
                    return [];
                });
            return xx;
        }
        return [];
    }

    async function GetThongTinPTN(id) {
        if (!commonStatisJs.CheckNull(id) && id !== const_GuidEmpty) {
            let xx = $.getJSON('/api/DanhMuc/GaraAPI/' + 'PhieuTiepNhan_GetThongTinChiTiet?id=' + id).done()
                .then(function (x) {
                    if (x.res && x.dataSoure.length > 0) {
                        return x.dataSoure[0];
                    }
                    return {};
                });
            return xx;
        }
        return {};
    }

    self.SaoChepHD_KhuyenMai = async function (item, type) {
        const hdDB = await GetInforHD_fromDB(item.ID);
        if ($.isEmptyObject(hdDB)) return;

        var newHD = $.extend({}, hdDB);
        newHD.BH_NhanVienThucHiens = await GetChietKhauNV_byIDHoaDon(item.ID);
        newHD.BH_HoaDon_ChiTiet = [];

        var gtrisudung = formatNumberToFloat(item.GiaTriSDDV);
        if (gtrisudung > 0) {
            if (type === 0) {
                ShowMessage_Danger('Vui lòng không sao chép hóa đơn sử dụng gói dịch vụ');
                return false;
            }
        }

        var maHD = 'Copy' + hdDB.MaHoaDon;
        var phaiTT = formatNumberToFloat(hdDB.PhaiThanhToan);
        var khachdatra = formatNumberToFloat(hdDB.KhachDaTra);
        // nếu TT= điểm --> caculator DiemQuyDoi from TTBangDiem
        let diemquydoi = 0;
        let tiendoidiem = 0;
        if (self.ThietLap_TichDiem() !== null && self.ThietLap_TichDiem().length > 0) {
            tiendoidiem = hdDB.TienDoiDiem;
            diemquydoi = Math.floor(tiendoidiem * self.ThietLap_TichDiem()[0].DiemThanhToan / self.ThietLap_TichDiem()[0].TienThanhToan);
        }

        var obj = GetPTChietKhauHang_HeaderBH();
        newHD.PTChietKhauHH = obj.PTChietKhauHH;
        newHD.HeaderBH_GiaTriPtram = 0;
        newHD.HeaderBH_Type = 1;
        // reset infor pthuc ThanhToan
        newHD.TienMat = 0;
        newHD.TienTheGiaTri = 0;
        newHD.TienATM = 0;
        newHD.TienGui = 0;
        newHD.ChuyenKhoan = 0;
        newHD.BangGiaWasChanged = false;
        newHD.NguoiTao = newHD.NguoiTao.toUpperCase();// vì ở BanLe.js check userlogin

        if (commonStatisJs.CheckNull(hdDB.CongThucBaoHiem)) {
            newHD.CongThucBaoHiem = 0;
        }
        if (commonStatisJs.CheckNull(hdDB.GiamTruThanhToanBaoHiem)) {
            newHD.GiamTruThanhToanBaoHiem = 0;
        }
        if (commonStatisJs.CheckNull(hdDB.MaPhieuTiepNhan)) {
            newHD.MaPhieuTiepNhan = '';
        }

        // assign again ID = constGuid_Empty at BanLe.js
        switch (type) {
            case 0:// saochep
                newHD.ID_HoaDon = null;
                newHD.KhachDaTra = 0;
                newHD.BaoHiemDaTra = 0;
                newHD.DaThanhToan = self.IsGara() ? 0 : hdDB.PhaiThanhToan;
                newHD.TTBangDiem = tiendoidiem;
                newHD.DiemQuyDoi = diemquydoi;
                newHD.DiemGiaoDichDB = 0;
                newHD.DiemGiaoDich = hdDB.DiemGiaoDich;
                newHD.TrangThaiHD = 1;
                newHD.TienMat = self.IsGara() ? 0 : hdDB.PhaiThanhToan;
                SetCache_ifGara('TN_copyHD');
                SetCache_ifNotGara(3);

                break;
            case 1:// update HDTamLuu
                maHD = newHD.MaHoaDon;
                newHD.TrangThaiHD = 3;
                newHD.TTBangDiem = 0;
                newHD.DiemQuyDoi = 0;
                newHD.DiemGiaoDichDB = 0;
                newHD.DaThanhToan = self.IsGara() ? 0 : phaiTT - khachdatra; // số tiền còn lại phaiTT --> bind at BanHang
                newHD.TienMat = self.IsGara() ? 0 : phaiTT - khachdatra;
                newHD.ID_TaiKhoanPos = null;
                newHD.ID_TaiKhoanChuyenKhoan = null;
                SetCache_ifGara('TN_updateHD');
                SetCache_ifNotGara(7);
                break;
            case 2:// updateHD daTT
                maHD = newHD.MaHoaDon;
                newHD.TrangThaiHD = 8;
                newHD.TTBangDiem = 0;
                newHD.DiemQuyDoi = 0;
                newHD.DiemGiaoDichDB = hdDB.DiemGiaoDich; // tru diem giaodich HD cu
                newHD.DaThanhToan = self.IsGara() ? 0 : phaiTT - khachdatra;
                newHD.TienMat = self.IsGara() ? 0 : phaiTT - khachdatra; // = số tiền còn lại phaiTT
                newHD.ID_TaiKhoanPos = null;
                newHD.ID_TaiKhoanChuyenKhoan = null;

                SetCache_ifGara('TN_updateHD');
                SetCache_ifNotGara(8);
                break;
        }
        var lstKMCTHD = localStorage.getItem('productKM_HoaDon');
        if (lstKMCTHD !== null) {
            lstKMCTHD = JSON.parse(lstKMCTHD);
        }
        else {
            lstKMCTHD = [];
        }
        // remove item old in KhuyenMai if same MaHoaDon && add new
        lstKMCTHD = $.grep(lstKMCTHD, function (x) {
            return x.MaHoaDon !== maHD;
        });

        var note_KMaiHD = '';
        if (self.BH_HoaDonChiTiets() !== null) {
            const cthdDB = await GetChiTietHD_fromDB(item.ID);
            const allComboHD = await vmThanhPhanCombo.GetAllCombo_byIDHoaDon(item.ID);
            Caculator_ChiTietHD(cthdDB);

            let giamgiaKM_HD = hdDB.KhuyeMai_GiamGia;
            newHD.Status = 1;
            newHD.MaHoaDonDB = maHD;
            newHD.YeuCau = 1;
            newHD.ChoThanhToan = false;
            newHD.StatusOffline = false;
            newHD.DVTinhGiam = 'VND';
            if (newHD.TongChietKhau > 0) {
                newHD.DVTinhGiam = '%';
            }
            newHD.HoanTraThuKhac = 0;
            newHD.PhaiThanhToanDB = 0;
            newHD.TongGiaGocHangTra = 0;
            newHD.TongChiPhiHangTra = 0;
            newHD.TongTienTra = 0;
            newHD.PTGiamDB = 0;
            newHD.TongGiamGiaDB = 0;
            newHD.HoanTraTamUng = 0;
            newHD.IsKhuyenMaiHD = false;
            newHD.IsOpeningKMaiHD = false;
            newHD.KhuyeMai_GiamGia = 0;
            newHD.TongGiamGiaKM_HD = hdDB.TongGiamGia + giamgiaKM_HD;
            newHD.PTThueDB = 0;
            newHD.TongThueDB = 0;
            newHD.TongTienHangChuaCK = self.TongTienHangChuaCK();
            newHD.TongGiamGiaHang = self.TongGiamGiaHang();

            newHD.DiemHienTai = 0;
            newHD.DiemCong = 0;
            newHD.SoDuDatCoc = 0;

            newHD.TongTienKhuyenMai_CT = 0;
            newHD.TongGiamGiaKhuyenMai_CT = 0;

            newHD.DiemKhuyenMai = 0;
            newHD.ID_NhomDTApplySale = null;
            newHD.IsActive = '';
            // Goi dich vu
            newHD.NgayApDungGoiDV = null;
            newHD.HanSuDungGoiDV = null;
            if (newHD.ID_ViTri !== const_GuidEmpty) {
                newHD.CreateTime = FormatDatetime_AMPM(new Date());
            }
            else {
                newHD.CreateTime = 0;
                newHD.ID_ViTri = null;
            }
            newHD.TenViTriHD = item.TenPhongBan;
            newHD.ChoThanhToan = false; // set default = false


            // order by SoThuTu ASC --> group Hang Hoa by LoHang
            var arrCTsort = cthdDB.sort(function (a, b) {
                var x = a.SoThuTu,
                    y = b.SoThuTu;
                return x < y ? -1 : x > y ? 1 : 0;
            });
            var arrIDQuiDoi = [];
            var cthdLoHang = [];
            for (let i = 0; i < arrCTsort.length; i++) {
                let cthd = $.extend({}, arrCTsort[i]);
                let idKhuyenMai = cthd.ID_KhuyenMai;

                // if muahang - giamhang (chinh no): push , nguoclai not push
                if (cthd.ID_TangKem !== null) {
                    if (idKhuyenMai === null || idKhuyenMai === const_GuidEmpty) {
                        continue;
                    }
                }
                cthd = SetDefaultPropertiesCTHD(cthd, hdDB.MaHoaDon, hdDB.LoaiHoaDon);
                cthd.SoLuongDaMua = 0;
                cthd.TienChietKhau = cthd.GiamGia;
                cthd.DVTinhGiam = '%';
                cthd.GiaBan = formatNumberToFloat(cthd.DonGia);
                if (cthd.TienChietKhau > 0 && cthd.PTChietKhau === 0) {
                    cthd.DVTinhGiam = 'VND';
                }
                cthd.ID_ViTri = hdDB.ID_ViTri;
                cthd.TenViTri = item.TenPhongBan;

                let quanLiTheoLo = cthd.QuanLyTheoLoHang;
                cthd.QuanLyTheoLoHang = quanLiTheoLo;
                cthd.DM_LoHang = [];
                cthd.LotParent = quanLiTheoLo;
                if (type === 2) {
                    cthd.TonKho = cthd.TonKho + cthd.SoLuong;
                }
                //saochep: khong gan id_baoduong
                if (type === 0) {
                    cthd.ID_LichBaoDuong = null;
                }

                let dateLot = GetNgaySX_NgayHH(cthd);
                cthd.NgaySanXuat = dateLot.NgaySanXuat;
                cthd.NgayHetHan = dateLot.NgayHetHan;

                // PhiDichVu, LaPTPhiDichVu (get from DB store)
                cthd.TongPhiDichVu = cthd.SoLuong * cthd.PhiDichVu;
                if (cthd.LaPTPhiDichVu) {
                    cthd.TongPhiDichVu
                        = Math.round(cthd.SoLuong * cthd.GiaBan * cthd.PhiDichVu / 100);
                }

                // get tpcombo
                let combo = $.grep(allComboHD, function (x) {
                    return x.ID_ParentCombo === cthd.ID_ParentCombo;
                });
                if (combo.length > 0) {
                    cthd.ThanhPhanComBo = combo;
                    cthd = AssignThanhPhanComBo_toCTHD(cthd);
                }
                else {
                    cthd.ThanhPhanComBo = [];
                }

                cthd = AssignNVThucHien_toCTHD(cthd);
                cthd = AssignTPDinhLuong_toCTHD(cthd);

                // check KhuyenMai
                if (idKhuyenMai !== null && idKhuyenMai !== '00000000-0000-0000-0000-000000000000') {
                    let itemKM = $.grep(self.KM_KMApDung(), function (x) {
                        return x.ID_KhuyenMai === idKhuyenMai;
                    });
                    if (self.ThietLap().KhuyenMai === true && CheckKM_IsApDung(hdDB.ID_NhanVien, hdDB) && itemKM.length > 0) {
                        // if TangKem of HoaDon (ID_TangKem = null, ID_KhuyenMai !=null)
                        // if TangKem of HangHoa (ID_TangKem = ID_QuiDoi of HangTang, ID_KhuyenMai == null)
                        // save cache KhuyenMai of HoaDon
                        if (cthd.TangKem && $.inArray(itemKM[0].HinhThuc, [11, 12, 13, 14]) > -1) {// neu khuyenmai hoadon
                            // if was push KhuyenMai HoaDon --> not push 
                            for (let m = 0; m < lstKMCTHD.length; m++) {
                                if (lstKMCTHD[m].ID_KhuyenMai === idKhuyenMai && lstKMCTHD[m].MaHoaDon === cthd.MaHoaDon) {
                                    continue;
                                }
                            }
                            // find all Hang KhuyenMai of HoaDon
                            let hangTangHoaDon = $.grep(cthd, function (x) {
                                return x.ID_KhuyenMai === idKhuyenMai;
                            });
                            let exitsKM = $.grep(lstKMCTHD, function (x) {
                                return x.ID_KhuyenMai === hdDB.ID_KhuyenMai;
                            })
                            if (exitsKM.length === 0) {
                                let noteDetail = '';
                                for (let m = 0; m < hangTangHoaDon.length; m++) {
                                    // assign proprties hangTangHoaDon
                                    hangTangHoaDon[m] = SetDefaultPropertiesCTHD(hangTangHoaDon[m], maHD, hdDB.LoaiHoaDon);
                                    hangTangHoaDon[m].SoLuongDaMua = 0;
                                    hangTangHoaDon[m].TienChietKhau = hangTangHoaDon[m].GiamGia;
                                    hangTangHoaDon[m].DVTinhGiam = '%';
                                    hangTangHoaDon[m].GiaBan = hangTangHoaDon[m].DonGia;
                                    hangTangHoaDon[m].ThanhTien = (hangTangHoaDon[m].GiaBan - hangTangHoaDon[m].TienChietKhau) * hangTangHoaDon[m].SoLuong;
                                    if (hangTangHoaDon[m].TienChietKhau > 0 && hangTangHoaDon[m].PTChietKhau === 0) {
                                        hangTangHoaDon[m].DVTinhGiam = 'VND';
                                    }
                                    hangTangHoaDon[m].CssWarning = false;
                                    hangTangHoaDon[m].ID_ChiTietGoiDV = null;
                                    hangTangHoaDon[m].ThanhPhan_DinhLuong = [];
                                    hangTangHoaDon[m].TongPhiDichVu = 0;
                                    hangTangHoaDon[m].PhiDichVu = 0;
                                    hangTangHoaDon[m].LaPTPhiDichVu = false;
                                    hangTangHoaDon[m].ID_ViTri = null;
                                    hangTangHoaDon[m].TenViTri = '';
                                    hangTangHoaDon[m].ThoiGian = moment(new Date()).format('YYYY-MM-DD HH:mm');
                                    hangTangHoaDon[m].ThoiGianThucHien = 0;
                                    // lo hang
                                    hangTangHoaDon[m].QuanLyTheoLoHang = quanLiTheoLo;
                                    hangTangHoaDon[m].DM_LoHang = [];
                                    hangTangHoaDon[m].LotParent = quanLiTheoLo;
                                    noteDetail += hangTangHoaDon[m].SoLuong + ' ' + hangTangHoaDon[m].MaHangHoa + ', ';
                                    lstKMCTHD.push(hangTangHoaDon[m]);
                                    localStorage.setItem('productKM_HoaDon', JSON.stringify(lstKMCTHD))
                                }
                                // find CTKhuyenMai was ApDung in this HoaDon
                                let tongTienHang = 0;
                                let isGiamGiaPT = 0;
                                let gtriGiamGia = 0;
                                // sort DM_KhuyenMai_ChiTiet by TongTienHang (get DM gan nhat voi TongTienHang)
                                itemKM[0].DM_KhuyenMai_ChiTiet = itemKM[0].DM_KhuyenMai_ChiTiet.sort(function (a, b) {
                                    let x = a.TongTienHang, y = b.TongTienHang;
                                    return x > y ? 1 : x < y ? -1 : 0;
                                });
                                for (let j = 0; j < itemKM[0].DM_KhuyenMai_ChiTiet.length; j++) {
                                    if (itemKM[0].DM_KhuyenMai_ChiTiet[j].TongTienHang <= hdDB.TongTienHang) {
                                        gtriGiamGia = itemKM[0].DM_KhuyenMai_ChiTiet[j].GiamGia;
                                        tongTienHang = itemKM[0].DM_KhuyenMai_ChiTiet[j].TongTienHang;
                                        isGiamGiaPT = itemKM[0].DM_KhuyenMai_ChiTiet[j].GiamGiaTheoPhanTram;
                                    }
                                }
                                note_KMaiHD = itemKM[0].TenKhuyenMai + ': Tổng tiền hàng từ ' + formatNumber(tongTienHang);
                                switch (itemKM[0].HinhThuc) {
                                    case 12:// tang hang
                                        note_KMaiHD += ' tặng ' + Remove_LastComma(noteDetail);
                                        break;
                                    case 13:// giam gia hang
                                        newHD.KhuyeMai_GiamGia = giamgiaKM_HD;
                                        note_KMaiHD += ' giảm giá '.concat(isGiamGiaPT ? gtriGiamGia + ' %' : formatNumber(gtriGiamGia) + ' Đ', ' cho ', Remove_LastComma(noteDetail));
                                        break;
                                }
                                // update infor cache HoaDon when apply KhuyenMai
                                newHD.KhuyenMai_GhiChu = note_KMaiHD;
                                newHD.IsKhuyenMaiHD = true;
                                newHD.IsOpeningKMaiHD = true;
                            }
                            continue; // not add hangtangHoaDon in cache CTHD
                        }
                        else {
                            // find hang tang kem of this HangHoa
                            let hhTangKem = $.grep(arrCTsort, function (x) {
                                return x.ID_TangKem === cthd.ID_DonViQuiDoi;
                            });
                            let txtKhuyenMai_Last = '';
                            // kmai theo hanghoa or diemcong
                            if (hhTangKem.length > 0 || cthd.DiemKhuyenMai > 0) {
                                // chi add neu khong phai khuyenmai congdiem
                                if (cthd.TangKem === false && cthd.DiemKhuyenMai === 0) {
                                    // get all HangTang of this HangHoa
                                    for (let m = 0; m < hhTangKem.length; m++) {
                                        // assign proprties hhTangKem
                                        hhTangKem[m] = SetDefaultPropertiesCTHD(hhTangKem[m], hdDB.MaHoaDon, hdDB.LoaiHoaDon);
                                        hhTangKem[m].SoLuongDaMua = 0;
                                        hhTangKem[m].TienChietKhau = hhTangKem[m].GiamGia;
                                        hhTangKem[m].DVTinhGiam = '%';
                                        hhTangKem[m].GiaBan = hhTangKem[m].DonGia;
                                        hhTangKem[m].ThanhTien = (hhTangKem[m].GiaBan - hhTangKem[m].TienChietKhau) * hhTangKem[m].SoLuong;
                                        if (hhTangKem[m].TienChietKhau > 0 && hhTangKem[m].PTChietKhau === 0) {
                                            hhTangKem[m].DVTinhGiam = 'VND';
                                        }
                                        hhTangKem[m].CssWarning = false;
                                        hhTangKem[m].ID_ChiTietGoiDV = null;
                                        hhTangKem[m].ThanhPhan_DinhLuong = [];
                                        hhTangKem[m].TongPhiDichVu = 0;
                                        hhTangKem[m].PhiDichVu = 0;
                                        hhTangKem[m].LaPTPhiDichVu = false;
                                        hhTangKem[m].ID_ViTri = null;
                                        hhTangKem[m].TenViTri = '';
                                        hhTangKem[m].ThoiGian = moment(new Date()).format('YYYY-MM-DD HH:mm');
                                        hhTangKem[m].ThoiGianThucHien = 0;

                                        hhTangKem[m].QuanLyTheoLoHang = quanLiTheoLo;
                                        hhTangKem[m].DM_LoHang = [];
                                        hhTangKem[m].LotParent = quanLiTheoLo;
                                        cthd.HangHoa_KM.push(hhTangKem[m]);
                                        // VD: 3 hang 001, 1 hang 002
                                        txtKhuyenMai_Last += hhTangKem[m].SoLuong + ' ' + hhTangKem[m].MaHangHoa + ', ';
                                    }
                                    txtKhuyenMai_Last = Remove_LastComma(txtKhuyenMai_Last);
                                }

                                // find all Hang with same ID_KhuyenMai
                                let cthd_sameKM = $.grep(cthd, function (x) {
                                    return x.ID_KhuyenMai === idKhuyenMai;
                                });
                                let textKM_First = '';
                                for (let k = 0; k < cthd_sameKM.length; k++) {
                                    textKM_First += cthd_sameKM[k].SoLuong + ' ' + cthd_sameKM[k].MaHangHoa + ', ';
                                }
                                textKM_First = 'Khi mua ' + Remove_LastComma(textKM_First);
                                // set ghichu KhuyenMai for CTHD
                                let isGiamGiaPTram = false;
                                let gtriGiamGia = 0;
                                let giaKhuyenMai = 0;
                                let loaiKM_note = '';
                                let soluongMua_ThucTe = 0;
                                let itemCTApDung = [];
                                let arrNhomChilds = [];
                                // sort DM_KhuyenMai_ChiTiet by SoLuongMua (get DM gan nhat voi SoLuongMua)
                                itemKM[0].DM_KhuyenMai_ChiTiet = itemKM[0].DM_KhuyenMai_ChiTiet.sort(function (a, b) {
                                    var x = a.SoLuongMua, y = b.SoLuongMua;
                                    return x > y ? 1 : x < y ? -1 : 0;
                                });

                                for (let k = 0; k < itemKM[0].DM_KhuyenMai_ChiTiet.length; k++) {
                                    let itemCT = itemKM[0].DM_KhuyenMai_ChiTiet[k];
                                    // check ID_QuiDoi Mua thuoc KMai {ID_QuiDoiMua OR ID_NhomHHMua }
                                    arrNhomChilds = GetAll_IDNhomChild_ofNhomHH([itemCT.ID_NhomHangHoaMua]);
                                    if ($.inArray(cthd.ID_DonViQuiDoi, itemKM[0].ID_QuyDoiMuas) > -1
                                        || $.inArray(cthd.ID_NhomHangHoa, arrNhomChilds) > -1) {
                                        // if Kmai by ID_QuiDoi
                                        if (cthd.ID_DonViQuiDoi === itemCT.ID_DonViQuiDoiMua) {
                                            //isTangNhom = false;
                                            itemCTApDung = itemCT;
                                        }
                                        else {
                                            // Kmai by Nhom
                                            itemCTApDung = itemCT;
                                        }
                                        if (itemCTApDung != []) {
                                            break;
                                        }
                                    }
                                }
                                if (itemCTApDung != []) {
                                    isGiamGiaPTram = itemCTApDung.GiamGiaTheoPhanTram;
                                    gtriGiamGia = itemCTApDung.GiamGia;
                                    giaKhuyenMai = itemCTApDung.GiaKhuyenMai;
                                    isGiamGiaPTram = itemCTApDung.GiamGiaTheoPhanTram;
                                    // KMai by Nhom
                                    if (itemCTApDung.ID_NhomHangHoaMua !== null) {
                                        // get all CTHD thuoc nhom KM --> tinh soluong mua
                                        for (let n = 0; n < cthd.length; n++) {
                                            if ($.inArray(cthd[n].ID_NhomHangHoa, arrNhomChilds) > -1) {
                                                soluongMua_ThucTe += cthd[n].SoLuong;
                                            }
                                        }
                                    }
                                    else {
                                        // Kmai by ID_QuiDoi
                                        soluongMua_ThucTe = cthd.SoLuong;
                                    }
                                }
                                loaiKM_note = textKM_First;
                                switch (itemKM[0].HinhThuc) {
                                    case 21:
                                        if (isGiamGiaPTram) {
                                            loaiKM_note += ' giảm giá ' + gtriGiamGia + '% cho ' + txtKhuyenMai_Last;
                                        }
                                        else {
                                            loaiKM_note += ' giảm giá ' + gtriGiamGia + ' cho ' + txtKhuyenMai_Last;
                                        }
                                        break;
                                    case 22:
                                        loaiKM_note += ' tặng ' + txtKhuyenMai_Last;
                                        break;
                                    case 23:
                                        if (isGiamGiaPTram) {
                                            loaiKM_note += ' tặng ' + gtriGiamGia + '% điểm ';
                                        }
                                        else {
                                            // nhân theo Soluong Mua (chi tinh Soluong tai thoi diem mua hien tai)
                                            gtriGiamGia = Math.floor(gtriGiamGia * Math.floor(soluongMua_ThucTe / itemCTApDung.SoLuongMua));
                                            loaiKM_note += ' tặng ' + gtriGiamGia + ' điểm ';
                                        }
                                        break;
                                    case 24:
                                        loaiKM_note += ' giá ' + formatNumber(giaKhuyenMai);
                                        break;
                                }
                                // if hanghoa same ID_KhuyenMai --> neu da add KMai: not assign IsOpeningKMai, ISKhuyenMai = true
                                let existKM = $.grep(cthd, function (x) {
                                    return x.ID_KhuyenMai === idKhuyenMai && x.IsKhuyenMai === true;
                                });
                                if (existKM.length === 0) {
                                    cthd.IsOpeningKMai = true;
                                    cthd.IsKhuyenMai = true;
                                    cthd.TenKhuyenMai = itemKM[0].TenKhuyenMai + ': ' + loaiKM_note;
                                }
                            }// end if hangTangKem > 0
                        } // end else KM HangHoa
                    } // end if IsApDung
                }
                // check exist in cthdLoHang
                if ($.inArray(cthd.ID_DonViQuiDoi, arrIDQuiDoi) === -1) {
                    arrIDQuiDoi.unshift(cthd.ID_DonViQuiDoi);
                    cthd.SoThuTu = cthdLoHang.length + 1;
                    cthd.IDRandom = CreateIDRandom('RandomCT_');
                    if (quanLiTheoLo) {
                        // push DM_Lo
                        let objLot = $.extend({}, cthd);
                        objLot.HangCungLoais = [];
                        objLot.DM_LoHang = [];
                        cthd.DM_LoHang.push(objLot);
                    }
                    cthdLoHang.push(cthd);
                }
                else {
                    // find in cthdLoHang with same ID_QuiDoi
                    for (let j = 0; j < cthdLoHang.length; j++) {
                        if (cthdLoHang[j].ID_DonViQuiDoi === cthd.ID_DonViQuiDoi) {
                            if (quanLiTheoLo) {
                                // push DM_Lo
                                let objLot = $.extend({}, cthd);
                                objLot.LotParent = false;
                                objLot.HangCungLoais = [];
                                objLot.DM_LoHang = [];
                                objLot.IDRandom = CreateIDRandom('RandomCT_');
                                cthdLoHang[j].DM_LoHang.push(objLot);
                            }
                            else {
                                cthd.IDRandom = CreateIDRandom('RandomCT_');
                                cthd.LaConCungLoai = true;
                                cthdLoHang[j].HangCungLoais.push(cthd);
                            }
                            break;
                        }
                    }
                }
            }

            let cacheCP = localStorage.getItem('lcChiPhi');
            if (cacheCP !== null) {
                cacheCP = JSON.parse(cacheCP);
                // remove & add again
                cacheCP = $.grep(cacheCP, function (x) {
                    return x.ID_HoaDon !== item.ID;
                });
            }
            else {
                cacheCP = [];
            }
            await VueChiPhi.CTHD_GetChiPhiDichVu([item.ID]);
            if (VueChiPhi.ListChiPhi.length > 0) {
                let arrCP = $.extend([], true, VueChiPhi.ListChiPhi);
                for (let m = 0; m < arrCP.length; m++) {
                    let for1 = arrCP[m];
                    for (let n = for1.ChiTiets.length - 1; n > -1; n--) {
                        let for2 = for1.ChiTiets[n];
                        if (for2.ID_NhaCungCap === null) {
                            for1.ChiTiets.splice(n, 1);
                        }
                    }
                    cacheCP.push(for1);
                }
                for (let m = cacheCP.length - 1; m > -1; m--) {
                    if (cacheCP[m].ChiTiets.length === 0) {
                        cacheCP.splice(m, 1);
                    }
                }
            }
            if (cacheCP.length > 0) {
                localStorage.setItem('lcChiPhi', JSON.stringify(cacheCP));
            }

            // sort CTHD by SoThuTu desc
            cthdLoHang = cthdLoHang.sort(function (a, b) {
                var x = a.SoThuTu, y = b.SoThuTu;
                return x < y ? 1 : x > y ? -1 : 0;
            });
            // tinh ThoiGianThucHien DichVu
            var totalTime = 0;
            if (newHD.ID_ViTri !== null) {
                for (let i = 0; i < cthdLoHang.length; i++) {
                    totalTime += cthdLoHang[i].ThoiGianThucHien;
                }
            }
            newHD.ThoiGianThucHien = totalTime;
            // chietkhau NV hoadon
            if (type !== 1) {
                for (let k = 0; k < newHD.BH_NhanVienThucHiens.length; k++) {
                    newHD.BH_NhanVienThucHiens[k].IDRandom = CreateIDRandom('CKHD_');
                    newHD.BH_NhanVienThucHiens[k].ChietKhauMacDinh = newHD.BH_NhanVienThucHiens[k].PT_ChietKhau;
                    if (newHD.BH_NhanVienThucHiens[k].TinhChietKhauTheo === 3)
                        newHD.BH_NhanVienThucHiens[k].ChietKhauMacDinh = newHD.BH_NhanVienThucHiens[k].TienChietKhau / newHD.BH_NhanVienThucHiens[k].HeSo;
                }
            }
            else {
                // update HDTamLuu: khong get chietkhau NV, vi ThucThu HoaDon co the bi thay doi --> chietkhau old se khong con dung
                newHD.BH_NhanVienThucHiens = [];
            }
            // khuyenMai: hinhthuc 11, 14: giam gia HD, cong diem HD (todo 14: congdiem)
            if (newHD.ID_KhuyenMai !== null && hdDB.ID_KhuyenMai !== '00000000-0000-0000-0000-000000000000') {
                let itemKM = $.grep(self.KM_KMApDung(), function (x) {
                    return x.ID_KhuyenMai === hdDB.ID_KhuyenMai;
                });
                var exitsKM2 = $.grep(lstKMCTHD, function (x) {
                    return x.ID_KhuyenMai === hdDB.ID_KhuyenMai;
                })
                // if not exist KM in hangKM _HoaDon
                if (exitsKM2.length === 0 && self.ThietLap().KhuyenMai === true && CheckKM_IsApDung(hdDB.ID_NhanVien, newHD) && itemKM.length > 0) {
                    // find CTKhuyenMai was ApDung in this HoaDon
                    let tongTienHang = 0;
                    let isGiamGiaPT = 0;
                    let gtriGiamGia = 0;
                    // sort DM_KhuyenMai_ChiTiet by TongTienHang (get DM gan nhat voi TongTienHang)
                    itemKM[0].DM_KhuyenMai_ChiTiet = itemKM[0].DM_KhuyenMai_ChiTiet.sort(function (a, b) {
                        var x = a.TongTienHang, y = b.TongTienHang;
                        return x > y ? 1 : x < y ? -1 : 0;
                    });
                    for (let j = 0; j < itemKM[0].DM_KhuyenMai_ChiTiet.length; j++) {
                        if (itemKM[0].DM_KhuyenMai_ChiTiet[j].TongTienHang <= hdDB.TongTienHang) {
                            gtriGiamGia = itemKM[0].DM_KhuyenMai_ChiTiet[j].GiamGia;
                            tongTienHang = itemKM[0].DM_KhuyenMai_ChiTiet[j].TongTienHang;
                            isGiamGiaPT = itemKM[0].DM_KhuyenMai_ChiTiet[j].GiamGiaTheoPhanTram;
                        }
                    }
                    note_KMaiHD = itemKM[0].TenKhuyenMai + ': Tổng tiền hàng từ ' + formatNumber(tongTienHang);
                    switch (itemKM[0].HinhThuc) {
                        case 11:
                            note_KMaiHD += ' giảm giá ' + gtriGiamGia + ' ' + (isGiamGiaPT ? '%' : 'Đ') + ' cho hóa đơn';
                            break;
                        case 14:
                            note_KMaiHD += ' tặng ' + gtriGiamGia + ' ' + (isGiamGiaPT ? ' % điểm' : ' điểm') + ' cho hóa đơn';
                            break;
                    }
                    // update infor cache HoaDon when apply KhuyenMai
                    newHD.KhuyenMai_GhiChu = note_KMaiHD;
                    newHD.IsKhuyenMaiHD = true;
                    newHD.IsOpeningKMaiHD = true;
                    newHD.KhuyeMai_GiamGia = giamgiaKM_HD;
                }
            }
            delete newHD['BH_HoaDon_ChiTiet']
            // save cache HoaDon after check KhuyenMai
            localStorage.setItem('lcHDSaoChep', JSON.stringify(newHD));
            localStorage.setItem('lcCTHDSaoChep', JSON.stringify(cthdLoHang));

            CheckExistCacheHD_andRemove(item.ID);
            self.gotoGara();
        }
        else {
            ShowMessage_Danger('Không có chi tiết hóa đơn');
            return false;
        }
    }

    function CheckExistCacheHD_andRemove(idHoaDon) {
        let hdCacheName = '', cthdCacheName = '';
        switch (parseInt(self.NganhKinhDoanh())) {
            case 1:
                hdCacheName = 'lstHDLe';
                cthdCacheName = 'lstCTHDLe';
                break;
            case 2:
                hdCacheName = 'gara_lstHDLe';
                cthdCacheName = 'gara_lstCTHDLe';
                break;
        }

        var lstHD = localStorage.getItem(hdCacheName);
        var cthd = localStorage.getItem(cthdCacheName);
        if (lstHD !== null) {
            lstHD = JSON.parse(lstHD);
        }
        else {
            lstHD = [];
        }
        if (cthd !== null) {
            cthd = JSON.parse(cthd);
        }
        else {
            cthd = [];
        }
        let ex = $.grep(lstHD, function (x) {
            return x.ID === idHoaDon;
        });
        if (ex.length > 0) {
            lstHD = $.grep(lstHD, function (x) {
                return x.ID !== idHoaDon;
            });
            cthd = $.grep(cthd, function (x) {
                return x.IDRandomHD !== ex[0].IDRandom;
            });
            localStorage.setItem(hdCacheName, JSON.stringify(lstHD));
            localStorage.setItem(cthdCacheName, JSON.stringify(cthd));
        }
    }

    function AssignNVThucHien_toCTHD(itemCT) {
        var listBH_NVienThucHienOld = itemCT.BH_NhanVienThucHien;
        itemCT.BH_NhanVienThucHien = [];// reset BH_NhanVienThucHien old, and add again
        var nvTH = '';
        var nvTV = '';
        var nvTH_Print = '';
        var nvTV_Print = '';
        for (let j = 0; j < listBH_NVienThucHienOld.length; j++) {
            let itemFor = listBH_NVienThucHienOld[j];
            let isNVThucHien = itemFor.ThucHien_TuVan;
            let tienCK = itemFor.TienChietKhau;
            let gtriPtramCK = itemFor.PT_ChietKhau;
            let isPTram = gtriPtramCK > 0 ? true : false;
            let gtriCK_TH = 0;
            let gtriCK_TV = 0;
            let tacVu = 1;
            let ckMacDinh = gtriPtramCK;
            if (isNVThucHien) {
                if (itemFor.TheoYeuCau) {
                    tacVu = 3;  // thuchien theo yeucau
                }
                else {
                    tacVu = 1;
                }
                if (isPTram) {
                    gtriCK_TH = gtriPtramCK;
                    nvTH += itemFor.TenNhanVien + ' (' + gtriCK_TH + ' %), ';
                }
                else {
                    gtriCK_TH = tienCK;
                    nvTH += itemFor.TenNhanVien + ' (' + formatNumber(gtriCK_TH) + ' đ), ';
                    ckMacDinh = tienCK / itemFor.HeSo / itemCT.SoLuong;
                }
                nvTH_Print += itemFor.TenNhanVien + ', ';
            }
            else {
                tacVu = 2;
                if (isPTram) {
                    gtriCK_TV = gtriPtramCK;
                    nvTV += itemFor.TenNhanVien + ' (' + gtriCK_TV + ' %), ';
                }
                else {
                    gtriCK_TV = tienCK;
                    nvTV += itemFor.TenNhanVien + ' (' + formatNumber(gtriCK_TV) + ' đ), ';
                    ckMacDinh = tienCK / itemFor.HeSo / itemCT.SoLuong;
                }
                nvTV_Print += itemFor.TenNhanVien + ', ';
            }
            var idRandom = CreateIDRandom('IDRandomCK_');
            var itemNV = {
                IDRandom: idRandom,
                ID_NhanVien: itemFor.ID_NhanVien,
                TenNhanVien: itemFor.TenNhanVien,
                ThucHien_TuVan: isNVThucHien,
                TienChietKhau: tienCK,
                PT_ChietKhau: gtriPtramCK,
                TheoYeuCau: itemFor.TheoYeuCau,
                TacVu: tacVu,
                HeSo: itemFor.HeSo,
                TinhChietKhauTheo: itemFor.TinhChietKhauTheo,
                ChietKhauMacDinh: ckMacDinh,
                TinhHoaHongTruocCK: itemFor.TinhHoaHongTruocCK,
            }
            itemCT.BH_NhanVienThucHien.push(itemNV);
        }

        itemCT.GhiChu_NVThucHien = nvTH === '' ? '' : 'Thực hiện: ' + Remove_LastComma(nvTH);
        itemCT.GhiChu_NVThucHienPrint = nvTH_Print === '' ? '' : Remove_LastComma(nvTH_Print);
        itemCT.GhiChu_NVTuVan = nvTV === '' ? '' : 'Tư vấn: ' + Remove_LastComma(nvTV);
        itemCT.GhiChu_NVTuVanPrint = nvTV_Print === '' ? '' : Remove_LastComma(nvTV_Print);
        itemCT.HoaHongTruocChietKhau =
            listBH_NVienThucHienOld != null && listBH_NVienThucHienOld.length > 0 ?
                listBH_NVienThucHienOld[0].TinhHoaHongTruocCK : itemCT.HoaHongTruocChietKhau;
        return itemCT;
    }
    function AssignTPDinhLuong_toCTHD(itemCT) {
        if (itemCT.ThanhPhan_DinhLuong !== null && itemCT.ThanhPhan_DinhLuong !== undefined) {
            for (let k = 0; k < itemCT.ThanhPhan_DinhLuong.length; k++) {
                let tpDL = itemCT.ThanhPhan_DinhLuong[k];
                itemCT.ThanhPhan_DinhLuong[k].STT = k + 1;
                itemCT.ThanhPhan_DinhLuong[k].isDefault = false;
                itemCT.ThanhPhan_DinhLuong[k].IDRandom = CreateIDRandom('TPDL_');
                itemCT.ThanhPhan_DinhLuong[k].SoLuongQuyCach = tpDL.SoLuong * tpDL.QuyCach;
                itemCT.ThanhPhan_DinhLuong[k].SoLuongDinhLuong_BanDau = tpDL.SoLuong / itemCT.SoLuong;
                itemCT.ThanhPhan_DinhLuong[k].SoLuongMacDinh = itemCT.ThanhPhan_DinhLuong[k].SoLuongDinhLuong_BanDau; // assign = SoLuongDinhLuong_BanDau
                itemCT.ThanhPhan_DinhLuong[k].GiaVonAfter = tpDL.SoLuong * tpDL.GiaVon;
            }
        }
        else {
            itemCT.ThanhPhan_DinhLuong = [];
        }
        return itemCT;
    }

    function AssignThanhPhanComBo_toCTHD(itemCT) {
        if (itemCT.ThanhPhanComBo !== null && itemCT.ThanhPhanComBo !== undefined) {
            for (let k = 0; k < itemCT.ThanhPhanComBo.length; k++) {
                let combo = itemCT.ThanhPhanComBo[k];
                combo.IDRandom = CreateIDRandom('combo_');
                combo = AssignNVThucHien_toCTHD(combo);
                combo = AssignTPDinhLuong_toCTHD(combo);
                let dateLot1 = GetNgaySX_NgayHH(combo);
                combo.NgaySanXuat = dateLot1.NgaySanXuat;
                combo.NgayHetHan = dateLot1.NgayHetHan;
                combo.LotParent = false;
                combo.DM_LoHang = [];

                combo.TongPhiDichVu = combo.PhiDichVu * combo.SoLuong;
                if (combo.LaPTPhiDichVu) {
                    combo.TongPhiDichVu = RoundDecimal(combo.PhiDichVu * combo.SoLuong * combo.DonGia / 100, 3);
                }

                combo.LoaiHoaDon = itemCT.LoaiHoaDon;
                combo.MaHoaDon = itemCT.MaHoaDon;
                combo.ID_ViTri = itemCT.ID_ViTri;
                combo.TenViTri = itemCT.TenPhongBan;

                combo.SoLuongMacDinh = itemCT.SoLuong === 0 ? combo.SoLuong : combo.SoLuong / itemCT.SoLuong;
                combo.SoLuongDaMua = 0;
                combo.CssWarning = false;
                combo.IsKhuyenMai = false;
                combo.IsOpeningKMai = false;
                combo.TenKhuyenMai = '';
                combo.HangHoa_KM = [];
                combo.UsingService = false;
                combo.ListDonViTinh = [];
                combo.ShowWarningQuyCach = false;
                combo.SoLuongQuyCach = 0;
                combo.HangCungLoais = [];
                combo.LaConCungLoai = false;
                combo.TimeStart = 0;
                combo.QuaThoiGian = 0;
                combo.TimeRemain = 0;
                combo.ThoiGian = moment(new Date()).format('YYYY-MM-DD HH:mm');
            }
        }
        else {
            itemCT.ThanhPhanComBo = [];
        }
        return itemCT;
    }

    // show infor HoaDon/PhieuThu/Chi
    self.Modal_HoaDons = ko.observableArray();
    self.TongSLHang = ko.observable(0);
    self.LoaiHoaDon_MoPhieu = ko.observable(0);
    self.MaHoaDon_MoPhieu = ko.observable('');

    self.ShowPopup_InforHD_PhieuThu = function (item, itHD) {
        self.LoaiHoaDon_MoPhieu(item.LoaiHoaDon);
        vmThanhToan.showModalUpdate(item.ID, itHD.ConNo);
    }

    self.ClickMoPhieu = function () {
        localStorage.setItem('FindHD', self.MaHoaDon_MoPhieu());
        var url = '';
        switch (self.LoaiHoaDon_MoPhieu()) {
            case 6:
                localStorage.setItem('FindHD', self.MaHoaDon_MoPhieu());
                url = "/#/Returns";
                break;
            case 11:
            case 12:
                localStorage.removeItem('FindHD');
                localStorage.setItem('FindMaPhieuChi', self.MaHoaDon_MoPhieu());
                url = '/#/CashFlow';
                break;
        }
        if (url !== '') {
            window.open(url);
        }
    }

    self.Goto_LoHang = function (item) {
        localStorage.setItem('FindLoHang', item.MaLoHang);
        var url = "/#/Shipment";
        window.open(url);
    }

    function GetHT_TichDiem() {
        ajaxHelper('/api/DanhMuc/HT_API/' + 'GetHT_CauHinh_TichDiemChiTiet?idDonVi=' + id_donvi, 'GET').done(function (obj) {
            if (obj.res === true) {
                self.ThietLap_TichDiem(obj.data);
            }
        });
    }

    self.ThongTinPhieuTiepNhan = ko.observable();
    self.HangMucSuaChuas = ko.observableArray();
    self.VatDungKemTheos = ko.observableArray();

    function GetInfor_PhieuTiepNhan(id) {
        if (id !== null) {
            $.getJSON('/api/DanhMuc/GaraAPI/' + 'PhieuTiepNhan_GetThongTinChiTiet?id=' + id).done(function (x) {
                if (x.res && x.dataSoure.length > 0) {
                    self.ThongTinPhieuTiepNhan(x.dataSoure[0]);
                }
                $.getJSON('/api/DanhMuc/GaraAPI/' + "PhieuTiepNhan_GetTinhTrangXe?id=" + id).done(function (o) {
                    if (o.res) {
                        let hm = o.dataSoure.hangmuc.map(function (item, index) {
                            return {
                                STT: index + 1,
                                TenHangMuc: item.TenHangMuc,
                                TinhTrang: item.TinhTrang,
                                PhuongAnSuaChua: item.PhuongAnSuaChua,
                                Anh: item.Anh,
                            }
                        });

                        let vd = o.dataSoure.vatdung.map(function (item, index) {
                            return {
                                STT: index + 1,
                                TieuDe: item.TieuDe,
                                SoLuong: item.SoLuong,
                                FileDinhKem: item.FileDinhKem,
                            }
                        });
                        self.HangMucSuaChuas(hm);
                        self.VatDungKemTheos(vd);
                    }
                });
            })
        }
        else {
            self.HangMucSuaChuas([]);
            self.VatDungKemTheos([]);
        }
    }

    self.showModalEditCKHoaDon = async function (item) {
        let tongTT = formatNumberToFloat(item.TongThanhToan);
        let butruTra = formatNumberToFloat(item.TongTienHDTra);
        let daTT = formatNumberToFloat(item.KhachDaTra) + formatNumberToFloat(item.BaoHiemDaTra)
            - formatNumberToFloat(item.TienDoiDiem) - formatNumberToFloat(item.ThuTuThe);
        let obj = {
            ID: item.ID,
            LoaiHoaDon: item.LoaiHoaDon,
            MaHoaDon: item.MaHoaDon,
            TongThanhToan: tongTT,
            TongTienThue: item.TongTienThue,
            ThucThu: daTT,
            DaThuTruoc: daTT,
            ConNo: tongTT - butruTra - daTT,
        }
        await vmHoaHongHoaDon.GetChietKhauHoaDon_byID(obj);
        vmHoaHongHoaDon.showModalUpdate(obj);
    }

    self.showModalEditCKDichVu = function (item) {
        vmChiTietHoaDon.showModalChiTietHoaDon(item.ID);
    }

    $('#ThuTienHoaDonModal').on('hidden.bs.modal', function () {
        if (vmThanhToan.saveOK) {
            SearchHoaDon();
        }
    })

    self.showTPComBo = function (item) {
        item.LoaiHoaDon = loaiHoaDon;
        if (item.LoaiHangHoa === 3) {
            vmThanhPhanCombo.showModalUpdate(item);
        }
        else {
            vmHDSC_chitietXuat.showModal(item);
        }
    }

    self.NhapHang = async function (item) {
        const cthdDB = await GetChiTietHD_fromDB(item.ID);
        if (cthdDB.length === 0) return;
        const allComboHD = await vmThanhPhanCombo.GetAllCombo_byIDHoaDon(item.ID);

        // get tpdluong 
        var arr = [];
        for (let i = 0; i < cthdDB.length; i++) {
            let forOut = cthdDB[i];
            switch (parseInt(forOut.LoaiHangHoa)) {
                case 1:
                    arr.push(forOut);
                    break;
                case 2:
                    if (!commonStatisJs.CheckNull(forOut.ThanhPhan_DinhLuong)) {
                        for (let j = 0; j < forOut.ThanhPhan_DinhLuong.length; j++) {
                            let forIn = forOut.ThanhPhan_DinhLuong[j];
                            arr.push(forIn);
                        }
                    }
                    break;
                case 3:
                    let combo = $.grep(allComboHD, function (x) {
                        return x.ID_ParentCombo === forOut.ID_ParentCombo;
                    });

                    if (combo.length > 0) {
                        for (let k = 0; k < combo.length; k++) {
                            let for1 = combo[k];
                            switch (parseInt(for1.LoaiHangHoa)) {
                                case 1:
                                    arr.push(for1);
                                    break;
                                case 2:
                                    if (!commonStatisJs.CheckNull(for1.ThanhPhan_DinhLuong)) {
                                        for (let j = 0; j < for1.ThanhPhan_DinhLuong.length; j++) {
                                            let forIn = for1.ThanhPhan_DinhLuong[j];
                                            arr.push(forIn);
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                    break;
            }
        }
        if (arr.length === 0) {
            commonStatisJs.ShowMessageDanger('Hóa đơn chỉ bao gồm dịch vụ. Không thể xuất kho');
            return;
        }

        let sum = arr.reduce(function (x, item) {
            return x + (item.SoLuong * item.GiaNhap);
        }, 0);

        let cthdLoHang = [], arrIDQuiDoi = [];
        for (let i = 0; i < arr.length; i++) {
            var ctNew = $.extend({}, arr[i]);
            delete ctNew["ID"];
            ctNew.TenDoiTuong = '';
            ctNew.TongTienHangChuaCK = 0;
            ctNew.TongGiamGiaHang = 0;
            ctNew.PTChietKhauHH = 0;
            ctNew.PTThueHD = 0;
            ctNew.TongGiamGia = 0;
            ctNew.TongTienThue = 0;
            ctNew.TongChietKhau = 0;
            ctNew.MaHoaDon = '';
            ctNew.DienGiai = item.DienGiai;
            ctNew.ID_DoiTuong = null;
            ctNew.NgayLapHoaDon = new Date();
            ctNew.ID_HoaDon = const_GuidEmpty;

            ctNew.TongTienHang = sum;
            ctNew.PhaiThanhToan = sum;
            ctNew.TongThanhToan = sum;
            ctNew.KhachDaTra = sum;
            ctNew.DaThanhToan = sum;
            ctNew.ID_NhanVien = item.ID_NhanVien;

            if (commonStatisJs.CheckNull(ctNew.ThuocTinh_GiaTri)) {
                ctNew.ThuocTinh_GiaTri = '';
            }
            if (commonStatisJs.CheckNull(ctNew.DonViTinh)) {
                ctNew.DonViTinh = [];
            }
            if (commonStatisJs.CheckNull(ctNew.TyLeChuyenDoi)) {
                ctNew.TyLeChuyenDoi = 1;
            }
            if (commonStatisJs.CheckNull(ctNew.MaLoHang)) {
                ctNew.MaLoHang = '';
            }

            let idLoHang = ctNew.ID_LoHang;
            let quanLiTheoLo = !commonStatisJs.CheckNull(ctNew.ID_LoHang);
            let ngaysx = ctNew.NgaySanXuat !== null ? moment(ctNew.NgaySanXuat).format('DD/MM/YYYY') : '';
            let hethan = ctNew.NgayHetHan !== null ? moment(ctNew.NgayHetHan).format('DD/MM/YYYY') : '';

            if (ngaysx === 'Invalid date') {
                ngaysx = '';
            }
            if (hethan === 'Invalid date') {
                hethan = '';
            }
            ctNew.NgaySanXuat = ngaysx;
            ctNew.NgayHetHan = hethan;

            ctNew.DM_LoHang = [];
            ctNew.ID_LoHang = idLoHang;
            ctNew.LotParent = quanLiTheoLo;
            ctNew.QuanLyTheoLoHang = quanLiTheoLo;
            ctNew.SoThuTu = cthdLoHang.length + 1;
            ctNew.HangCungLoais = [];
            ctNew.ThanhPhanComBo = [];
            ctNew.ThanhPhan_DinhLuong = [];
            ctNew.LaConCungLoai = false;
            ctNew.ID_ChiTietGoiDV = null;
            ctNew.ID_ChiTietDinhLuong = null;
            ctNew.DVTinhGiam = '%';
            ctNew.PTChietKhau = 0;
            ctNew.TienChietKhau = 0;
            ctNew.PTThue = 0;
            ctNew.TienThue = 0;
            ctNew.DonGia = ctNew.GiaNhap;
            ctNew.ThanhTien = ctNew.SoLuong * ctNew.GiaNhap;
            ctNew.ThanhToan = ctNew.ThanhTien;

            if ($.inArray(ctNew.ID_DonViQuiDoi, arrIDQuiDoi) === -1) {
                arrIDQuiDoi.unshift(ctNew.ID_DonViQuiDoi);
                ctNew.IDRandom = CreateIDRandom('CTHD_');
                if (quanLiTheoLo) {
                    // push DM_Lo
                    let objLot = $.extend({}, ctNew);
                    objLot.HangCungLoais = [];
                    objLot.DM_LoHang = [];
                    ctNew.DM_LoHang.push(objLot);
                }
                cthdLoHang.push(ctNew);
            }
            else {
                // find in cthdLoHang with same ID_QuiDoi
                for (let j = 0; j < cthdLoHang.length; j++) {
                    if (cthdLoHang[j].ID_DonViQuiDoi === ctNew.ID_DonViQuiDoi) {
                        if (quanLiTheoLo) {
                            let exLo = false;
                            for (let k = 0; k < cthdLoHang[j].DM_LoHang.length; k++) {
                                let forLot = cthdLoHang[j].DM_LoHang[k];
                                if (forLot.ID_LoHang === ctNew.ID_LoHang) {
                                    exLo = true;
                                    cthdLoHang[j].DM_LoHang[k].SoLuong = forLot.SoLuong + ctNew.SoLuong;
                                    cthdLoHang[j].DM_LoHang[k].ThanhTien = cthdLoHang[j].DM_LoHang[k].SoLuong * forLot.GiaNhap;
                                }
                            }
                            if (!exLo) {
                                let objLot = $.extend({}, ctNew);
                                objLot.LotParent = false;
                                objLot.HangCungLoais = [];
                                objLot.DM_LoHang = [];
                                objLot.IDRandom = CreateIDRandom('RandomCT_');
                                cthdLoHang[j].DM_LoHang.push(objLot);
                            }
                        }
                        else {
                            cthdLoHang[j].SoLuong = cthdLoHang[j].SoLuong + ctNew.SoLuong;
                            cthdLoHang[j].ThanhTien = cthdLoHang[j].SoLuong * cthdLoHang[j].GiaNhap;
                        }
                        break;
                    }
                }
            }
        }
        if (cthdLoHang.length > 0) {
            cthdLoHang[0].ID_HoaDonGoc = item.ID;
            cthdLoHang[0].ID_PhieuTiepNhan = item.ID_PhieuTiepNhan;
            cthdLoHang[0].MaPhieuTiepNhan = item.MaPhieuTiepNhan;
            cthdLoHang[0].BienSo = item.BienSo;
            cthdLoHang[0].MaHoaDonSuaChua = item.MaHoaDon;
        }

        localStorage.setItem('lc_CTSaoChep', JSON.stringify(cthdLoHang));
        localStorage.setItem('typeCacheNhapHang', 3);
        window.open('/#/PurchaseOrderItem2', '_blank');
    }

    self.CapNhatChiPhi = async function (item) {
        await VueChiPhi.CTHD_GetChiPhiDichVu([item.ID]);
        VueChiPhi.ShowModal(1);
    }

    self.XuatKho = function (item) {
        localStorage.setItem('lcXK_EditOpen', JSON.stringify([item]));
        localStorage.setItem('XK_createfrom', 3);
        window.open('/#/XuatKhoChiTiet');
    }

    self.Invoice_UpdateImg = function (item) {
        vmUpAnhHoaDon.InvoiceChosing = item;
        vmUpAnhHoaDon.isSaveToTemp = false;
        vmUpAnhHoaDon.GetListImgInvoiceDB(item.ID, "123");
        vmUpAnhHoaDon.showModalInsert();
    }
};
var modelGiaoDich = new ViewModelHD();
ko.applyBindings(modelGiaoDich, document.getElementById('divPage'));
function hidewait(o) {
    $('.' + o).append('<div id="wait"><img src="/Content/images/wait.gif" width="64" height="64" /><div class="happy-wait">' +
        ' </div>' +
        '</div>')
}
var arrIDchose = [];
function selectedVT(obj) {
    if ($(obj).children().length === 0) {
        $(obj).append('<i class="fa fa-check" aria-hidden="true"></i><i class="fa fa-times"></i>');
    }
    if ($(obj).children().length > 0) {
        arrIDchose.push($(obj).attr('id'));
    }
}
$(function () {
    $('input[type=text]').click(function () {
        $(this).select();
    });
})
$(function () {
    $('.daterange').daterangepicker({
        locale: {
            "format": 'DD/MM/YYYY',
            "separator": " - ",
            "applyLabel": "Tìm kiếm",
            "cancelLabel": "Hủy",
            "fromLabel": "Từ",
            "toLabel": "Đến",
            "customRangeLabel": "Custom",
            "daysOfWeek": [
                "CN",
                "T2",
                "T3",
                "T4",
                "T5",
                "T6",
                "T7"
            ],
            "monthNames": [
                "Tháng 1",
                "Tháng 2",
                "Tháng 3",
                "Tháng 4",
                "Tháng 5",
                "Tháng 6",
                "Tháng 7",
                "Tháng 8",
                "Tháng 9",
                "Tháng 10",
                "Tháng 11",
                "Tháng 12"
            ],
            "firstDay": 1
        }
    });
});
var arrIDCheck = [];
function SetCheckAll(obj) {
    var isChecked = $(obj).is(":checked");
    $('.check-group input[type=checkbox]').each(function () {
        $(this).prop('checked', isChecked);
    })
    if (isChecked) {
        $('.check-group input[type=checkbox]').each(function () {
            var thisID = $(this).attr('id');
            if (thisID !== undefined && !(jQuery.inArray(thisID, arrIDCheck) > -1)) {
                arrIDCheck.push(thisID);
            }
        });
    }
    else {
        $('.check-group input[type=checkbox]').each(function () {
            var thisID = $(this).attr('id');
            for (let i = 0; i < arrIDCheck.length; i++) {
                if (arrIDCheck[i] === thisID) {
                    arrIDCheck.splice(i, 1);
                    break;
                }
            }
        })
    }
    if (arrIDCheck.length > 0) {
        $('#divThaoTac').css("display", "inline-block");
        $('.choose-commodity').css("display", "inline-block").trigger("RemoveClassForButtonNew");
        $('#count').text(arrIDCheck.length);
    }
    else {
        $('#divThaoTac').hide();
        $('.choose-commodity').hide().trigger("addClassForButtonNew");
    }
}
function ChoseHoaDon(obj) {
    var thisID = $(obj).attr('id');
    if ($(obj).is(':checked')) {
        if ($.inArray(thisID, arrIDCheck) === -1) {
            arrIDCheck.push(thisID);
        }
    }
    else {
        //remove item in arrID
        arrIDCheck = arrIDCheck.filter(x => x !== thisID);
    }
    if (arrIDCheck.length > 0) {
        $('#divThaoTac').show();
        $('.choose-commodity').css("display", "inline-block").trigger("RemoveClassForButtonNew");
        $('#count').text(arrIDCheck.length);
    }
    else {
        $('#divThaoTac').hide();
        $('.choose-commodity').hide().trigger("addClassForButtonNew");
    }
    // count input is checked
    var countCheck = 0;
    $('#tb tr td.check-group input').each(function (x) {
        var id = $(this).attr('id');
        if ($.inArray(id, arrIDCheck) > -1) {
            countCheck += 1;
        }
    });
    // set check for header
    var ckHeader = $('#tb thead tr th:eq(0) input');
    var lenList = $('#tb tbody tr.prev-tr-hide').length;
    if (countCheck === lenList) {
        ckHeader.prop('checked', true);
    }
    else {
        ckHeader.prop('checked', false);
    }
}
function RemoveAllCheck() {
    $('input[type=checkbox]').prop('checked', false);
    arrIDCheck = [];
    $('#divThaoTac').hide();
    $('.choose-commodity').hide();
}

function ConvertMinutes_ToHourMinutes(sophut) {
    var div = RoundDecimal(sophut / 60);
    var hours = Math.floor(div);
    var minutes = formatNumber3Digit((div - hours) * 60);
    if (hours > 0) {
        return hours.toString().concat(' giờ ', minutes, ' phút');
    }
    return minutes.toString().concat(' phút');
}

