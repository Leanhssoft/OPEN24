﻿var vmThanhToanGara = new Vue({
    el: '#ThongTinThanhToanKHNCC',
    components: {
        'account-bank': cmpChoseAccountBank,
        'khoan-thu-chi': cmpChoseKhoanThu,
        'nvien-hoadon-search': cmpSearchNVDisscount,
    },
    created: function () {
        this.GuidEmpty = '00000000-0000-0000-0000-000000000000';
    },
    data: {
        saveOK: false,
        isLoading: false,
        isNew: true,
        formType: 1,
        IsShareDiscount: '2',
        LoaiChietKhauHD_NV: '2',
        RoleChange_ChietKhauNV: true,
        isCheckTraLaiCoc: false,
        ThietLap_TichDiem: {
            DuocThietLap: false,
            DiemThanhToan: 1,
            TienThanhToan: 1,
            TyLeDoiDiem: 1,
            TichDiemGiamGia: false,
            TichDiemHoaDonGiamGia: true,
        },
        itemChosing: {},

        theGiaTriCus: {
            TongNapThe: 0,
            SuDungThe: 0,
            HoanTraTheGiaTri: 0,
            SoDuTheGiaTri: 0,
            CongNoThe: 0,
        },

        inforHoaDon: {
            ID: null,
            LoaiDoiTuong: 1,// 1.kh, 2.ncc, 3.bh
            LoaiHoaDon: 1,
            ID_DoiTuong: null,
            ID_BaoHiem: null,
            SoDuDatCoc: 0,
            SoDuTheGiaTri: 0,
            HoanTraTamUng: 0,
            PhaiThanhToan: 0,
            PhaiThanhToanBaoHiem: 0,
            TongThanhToan: 0,
            TongTienThue: 0,
            TongTichDiem: 0,
            ThucThu: 0,
            ConNo: 0,
            DienGiai: '',
            MaDoiTuong: '',
            TenDoiTuong: '',
            TenBaoHiem: '',
            MaHoaDon: '',
            ID_DonVi: null,
            ID_NhanVien: null,
            NgayLapHoaDon: null,
            ID_PhieuTiepNhan: null,
        },
        GridNVienBanGoi_Chosed: [],
        PhieuThuKhach: {
            DaThanhToan: 0,
            DiemQuyDoi: 0,// phai gan de lay gtri khi update diem khachhang
        },
        PhieuThuBaoHiem: {},
        listData: {
            AccountBanks: [],
            KhoanThuChis: [],
            NhanViens: [],
            ChietKhauHoaDons: [],
        },
        PhieuThuKhachPrint: {},
        PhieuThuBaoHiemPrint: {},
        QRCode: {
            MaNganHang: '',
            TenNganHangCK: '',
            SoTaiKhoan: '',
            SoTien: '',
            MaPin: '',
            NoiDung: 'Thanh Toan Hoa Don'
        },
        QRCodeBH: {
            MaNganHang: '',
            SoTaiKhoan: '',
            SoTien: '',
            NoiDung: 'BH Thanh Toan Hoa Don'
        },
        LinkQR: ''
    },
    methods: {
        newPhieuThu: function (loaiDoiTuong) {
            return {
                LoaiDoiTuong: loaiDoiTuong,
                MaHoaDon: '',
                NgayLapHoaDon: moment(new Date()).format('YYYY-MM-DD HH:mm'),
                SoDuTheGiaTri: 0,
                TienDatCoc: 0,
                TienMat: 0,
                TienPOS: 0,
                TienCK: 0,
                TienTheGiaTri: 0,
                DiemQuyDoi: 0,
                TTBangDiem: 0,
                ID_TaiKhoanPos: null,
                ID_TaiKhoanChuyenKhoan: null,
                ID_NhanVien: null,
                ID_KhoanThuChi: null,
                NoHienTai: 0,
                PhaiThanhToan: 0,// sotien phaitt sau khi nhap tiencoc
                DaThanhToan: 0,
                TienThua: 0,
                ThucThu: 0,
                TenTaiKhoanPos: '',//= ten chu the
                TenTaiKhoanCK: '',
                SoTaiKhoanPos: '',
                SoTaiKhoanCK: '',
                TenNganHangPos: '',
                TenNganHangCK: '',
                HoanTraTamUng: 0,
            };
        },
        newNhanVien_ChietKhauHoaDon: function (itemCK, itemNV, exitChietKhau) {
            var self = this;
            var doanhThu = self.inforHoaDon.TongThanhToan - self.inforHoaDon.TongTienThue;
            var thucThu = self.PhieuThuKhach.ThucThu + self.PhieuThuBaoHiem.ThucThu;
            if (exitChietKhau) {
                var tinhCKTheo = parseInt(itemCK.TinhChietKhauTheo);
                var valChietKhau = itemCK.GiaTriChietKhau;
                var tienCK_NV = 0; // used to assign in Grid
                var ptramCK = 0;
                switch (tinhCKTheo) {
                    case 1:
                        ptramCK = valChietKhau;
                        tienCK_NV = Math.round((valChietKhau / 100) * thucThu);
                        break;
                    case 2:
                        ptramCK = valChietKhau;
                        tienCK_NV = Math.round((valChietKhau / 100) * doanhThu);
                        break;
                    case 3:
                        tienCK_NV = valChietKhau;
                        break;
                }
                return {
                    ID_NhanVien: itemNV.ID,
                    MaNhanVien: itemNV.MaNhanVien,
                    TenNhanVien: itemNV.TenNhanVien,
                    ThucHien_TuVan: false,
                    TheoYeuCau: false,
                    HeSo: 1,
                    TinhChietKhauTheo: tinhCKTheo.toString(),
                    TienChietKhau: tienCK_NV,
                    PT_ChietKhau: ptramCK,
                    ChietKhauMacDinh: valChietKhau,
                }
            }
            else {
                return {
                    ID_NhanVien: itemNV.ID,
                    MaNhanVien: itemNV.MaNhanVien,
                    TenNhanVien: itemNV.TenNhanVien,
                    ThucHien_TuVan: false,
                    TheoYeuCau: false,
                    HeSo: 1,
                    TinhChietKhauTheo: '2',
                    TienChietKhau: 0,
                    PT_ChietKhau: 0,
                    ChietKhauMacDinh: 0,
                }
            }
        },

        GetSoDuTheGiaTri: function (idDoiTuong) {
            var self = this;
            if (!commonStatisJs.CheckNull(idDoiTuong) && idDoiTuong !== const_GuidEmpty) {
                let datetime = moment(new Date()).add('days', 1).format('YYYY-MM-DD');
                $.getJSON("/api/DanhMuc/DM_DoiTuongAPI/Get_SoDuTheGiaTri_ofKhachHang?idDoiTuong=" + idDoiTuong + '&datetime=' + datetime, function (data) {
                    let sodu = 0;
                    if (data !== null && data.length > 0) {
                        let item0 = data[0];
                        sodu = item0.SoDuTheGiaTri;
                        sodu = sodu < 0 ? 0 : sodu;
                        self.theGiaTriCus.SoDuTheGiaTri = sodu;
                        self.theGiaTriCus.CongNoThe = item0.CongNoThe;
                        self.theGiaTriCus.TongNapThe = item0.TongThuTheGiaTri;
                        self.theGiaTriCus.SuDungThe = item0.SuDungThe;
                    }
                    if ($('#vmNKGoiBaoDuong').length) {
                        vmNKGoiBaoDuong.soduTheGiaTri = sodu;
                    }
                });
            }
        },

        SetDefault_TienTheGiaTri_ifHaveSoSu: function (phaiTT) {
            var self = this;
            var soduThe = self.theGiaTriCus.SoDuTheGiaTri - self.theGiaTriCus.CongNoThe; // chi dc su dung the voi so tien da nop
            if (soduThe > 0) {
                if (soduThe > phaiTT) {
                    return {
                        TienMat: 0, TienTheGiaTri: phaiTT
                    };
                }
                else {
                    return {
                        TienMat: phaiTT - soduThe, TienTheGiaTri: soduThe
                    };
                }
            }
            else {
                return {
                    TienMat: phaiTT, TienTheGiaTri: 0
                };
            }
        },
        showModalUpdate: function (hd, formType = 2) {// use at banle
            var self = this;
            self.isCheckTraLaiCoc = false;
            self.saveOK = false;
            self.isLoading = false;
            self.formType = formType;
            self.GridNVienBanGoi_Chosed = [];
            self.PhieuThuKhach = self.newPhieuThu(1);
            self.PhieuThuBaoHiem = self.newPhieuThu(3);
            self.PhieuThuKhachPrint = self.newPhieuThu(1);
            self.PhieuThuBaoHiemPrint = self.newPhieuThu(3);
            var tiendatcoc = 0, datt = 0, cantt = 0, tienmat = 0, tienpos = 0, tienck = 0, tiendiem = 0, diemquydoi = 0, tienthe = 0;
            var idPOS = null, idCK = null;
            var hoantra = hd.HoanTraTamUng;
            var soduDatCoc = hd.SoDuDatCoc;

            var lstHD = localStorage.getItem('lstHDLe');
            if (lstHD !== null) {
                lstHD = JSON.parse(lstHD);
            }
            else {
                lstHD = [];
            }
            console.log('hd ', hd)

            for (let i = 0; i < lstHD.length; i++) {
                let itFor = lstHD[i];
                if (itFor.IDRandom === hd.IDRandom) {
                    tienmat = itFor.TienMat;
                    tienpos = itFor.TienATM;
                    tienck = itFor.TienGui;
                    diemquydoi = itFor.DiemQuyDoi;
                    tiendiem = itFor.TTBangDiem;
                    tienthe = itFor.TienTheGiaTri;
                    idPOS = itFor.ID_TaiKhoanPos;
                    idCK = itFor.ID_TaiKhoanChuyenKhoan;

                    self.GridNVienBanGoi_Chosed = itFor.BH_NhanVienThucHiens;
                    break;
                }
            }

            if (hoantra > 0) {
                cantt = hoantra;
            }
            else {
                tiendatcoc = soduDatCoc;
                cantt = hd.PhaiThanhToan - tiendatcoc - hd.KhachDaTra;

                let isAgree = tienthe > 0 || tienpos > 0 || tienck > 0;
                if (self.theGiaTriCus.SoDuTheGiaTri > 0 && isAgree === false) {
                    let obj = self.SetDefault_TienTheGiaTri_ifHaveSoSu(hd.PhaiThanhToan);
                    tienthe = obj.TienTheGiaTri;
                    tienmat = obj.TienMat;
                }
            }
            datt = tienmat + tienpos + tienck + tiendiem + tienthe;

            self.inforHoaDon = hd;
            self.inforHoaDon.DaThanhToan = datt;
            self.inforHoaDon.ThucThu = datt - tiendiem - tienthe;

            self.PhieuThuKhach.TienDatCoc = formatNumber3Digit(tiendatcoc, 2);
            self.PhieuThuKhach.TienMat = formatNumber3Digit(tienmat, 2);
            self.PhieuThuKhach.TienPOS = formatNumber3Digit(tienpos, 2);
            self.PhieuThuKhach.TienCK = formatNumber3Digit(tienck, 2);
            self.PhieuThuKhach.TienTheGiaTri = formatNumber3Digit(tienthe, 2);
            self.PhieuThuKhach.TTBangDiem = formatNumber3Digit(tiendiem, 2);
            self.PhieuThuKhach.DiemQuyDoi = formatNumber3Digit(diemquydoi);

            self.PhieuThuKhach.PhaiThanhToan = cantt;
            self.PhieuThuKhach.HoanTraTamUng = hoantra;
            self.PhieuThuKhach.ThucThu = self.inforHoaDon.ThucThu;
            let tkPos = $.grep(self.listData.AccountBanks, function (x) {
                return x.ID === idPOS;
            });
            if (tkPos.length > 0) {
                self.ChangeAccountPOS(tkPos[0]);
            }
            else {
                self.ResetAccountPOS();
            }
            let tkCK = $.grep(self.listData.AccountBanks, function (x) {
                return x.ID === idCK;
            });
            if (tkCK.length > 0) {
                self.ChangeAccountCK(tkCK[0]);
            }
            else {
                self.ResetAccountCK();
            }
            $('#ThongTinThanhToanKHNCC').modal('show');
        },
        showModalThanhToan: function (hd, formType = 1) { // 1.gara, 2.banle, 3.thegiatri
            var self = this;
            self.isCheckTraLaiCoc = false;
            self.saveOK = false;
            self.isLoading = false;
            self.formType = formType;
            self.inforHoaDon = hd;
            self.GridNVienBanGoi_Chosed = [];
            self.PhieuThuKhach = self.newPhieuThu(1);
            self.PhieuThuBaoHiem = self.newPhieuThu(3);
            self.PhieuThuKhachPrint = self.newPhieuThu(1);
            self.PhieuThuBaoHiemPrint = self.newPhieuThu(3);
            console.log('gara-tt')

            var lstHD = localStorage.getItem('gara_lstHDLe');
            if (lstHD !== null) {
                lstHD = JSON.parse(lstHD);
            }
            else {
                lstHD = [];
            }

            for (let i = 0; i < lstHD.length; i++) {
                let itFor = lstHD[i];
                if (itFor.IDRandom === hd.IDRandom) {
                    self.GridNVienBanGoi_Chosed = itFor.BH_NhanVienThucHiens;
                    break;
                }
            }

            // set default tienmat phaitt for khachhang
            var tiendatcoc = 0, datt = 0, cantt = 0, tienmat = 0;
            var hoantra = hd.HoanTraTamUng;
            var soduDatCoc = hd.SoDuDatCoc;
            if (hoantra > 0) {
                datt = cantt = hoantra;

                // neu update hdold
                if (soduDatCoc > 0) {
                    self.isCheckTraLaiCoc = true;// set default tralaicoc: if sodu > 0

                    var hdUpdate_CanTT = hd.PhaiThanhToan - hd.KhachDaTra;
                    if (hd.KhachDaTra > 0) {
                        if (hdUpdate_CanTT > 0) {
                            if (hdUpdate_CanTT > soduDatCoc) {
                                tiendatcoc = soduDatCoc;
                            }
                            else {
                                tiendatcoc = hdUpdate_CanTT;
                            }
                        }
                    }
                    else {
                        tiendatcoc = hd.PhaiThanhToan;
                    }
                }
                tienmat = hoantra;
            }
            else {
                tiendatcoc = soduDatCoc;
                cantt = tienmat = hd.PhaiThanhToan - tiendatcoc - hd.KhachDaTra;
                datt = tienmat + tiendatcoc;
            }
            self.PhieuThuKhach.TienDatCoc = formatNumber3Digit(tiendatcoc, 2);
            self.PhieuThuKhach.TienMat = formatNumber3Digit(tienmat, 2);
            self.PhieuThuKhach.PhaiThanhToan = cantt;
            self.PhieuThuKhach.DaThanhToan = datt; // sum (mat + coc)
            self.PhieuThuKhach.HoanTraTamUng = hoantra; // used when edit kh_tiencoc
            self.PhieuThuKhach.ThucThu = self.inforHoaDon.ThucThu;// first show: set default ThucThu

            $('#ThongTinThanhToanKHNCC').modal('show');
        },
        changeTab: function (loaiDT) {
            var self = this;
            self.inforHoaDon.LoaiDoiTuong = loaiDT;
            self.UpdateChietKhauNV_ifChangeThucThu();
        },
        CaculatorDaThanhToan: function () {
            var self = this;
            var tiencoc = formatNumberToFloat(self.PhieuThuKhach.TienDatCoc);
            if (self.PhieuThuKhach.HoanTraTamUng > 0 && self.inforHoaDon.SoDuDatCoc > 0) {
                // neu chi tra tien: khong tinh tiencoc
                tiencoc = 0;
            }
            self.PhieuThuKhach.DaThanhToan = formatNumberToFloat(self.PhieuThuKhach.TienMat)
                + tiencoc
                + formatNumberToFloat(self.PhieuThuKhach.TienPOS)
                + formatNumberToFloat(self.PhieuThuKhach.TienCK)
                + formatNumberToFloat(self.PhieuThuKhach.TienTheGiaTri)
                + formatNumberToFloat(self.PhieuThuKhach.TTBangDiem);

            var thucthu = 0;
            if (self.PhieuThuKhach.HoanTraTamUng > 0) {
                thucthu = formatNumberToFloat(self.PhieuThuKhach.TienDatCoc);
                self.PhieuThuKhach.TienThua = self.PhieuThuKhach.DaThanhToan - self.PhieuThuKhach.PhaiThanhToan;
            }
            else {
                thucthu = formatNumberToFloat(self.PhieuThuKhach.TienMat)
                    + formatNumberToFloat(self.PhieuThuKhach.TienPOS)
                    + formatNumberToFloat(self.PhieuThuKhach.TienCK) +
                    + formatNumberToFloat(self.PhieuThuKhach.TienDatCoc);
                self.PhieuThuKhach.TienThua = self.PhieuThuKhach.DaThanhToan + self.inforHoaDon.KhachDaTra - self.inforHoaDon.PhaiThanhToan;
                if (self.PhieuThuKhach.TienThua > 0) {
                    thucthu = self.inforHoaDon.PhaiThanhToan
                        - self.inforHoaDon.KhachDaTra
                        - formatNumberToFloat(self.PhieuThuKhach.TienTheGiaTri)
                        - formatNumberToFloat(self.PhieuThuKhach.TTBangDiem);
                }
            }
            self.PhieuThuKhach.ThucThu = thucthu;
            self.Caculator_ThucThu();
            self.UpdateChietKhauNV_ifChangeThucThu();
        },
        BH_CaculatorDaThanhToan: function () {
            var self = this;
            self.PhieuThuBaoHiem.DaThanhToan = formatNumberToFloat(self.PhieuThuBaoHiem.TienMat)
                + formatNumberToFloat(self.PhieuThuBaoHiem.TienPOS)
                + formatNumberToFloat(self.PhieuThuBaoHiem.TienCK)
                + formatNumberToFloat(self.PhieuThuBaoHiem.TienTheGiaTri)
            self.PhieuThuBaoHiem.ThucThu = formatNumberToFloat(self.PhieuThuBaoHiem.TienMat)
                + formatNumberToFloat(self.PhieuThuBaoHiem.TienPOS)
                + formatNumberToFloat(self.PhieuThuBaoHiem.TienCK);
            self.PhieuThuBaoHiem.TienThua = self.PhieuThuBaoHiem.DaThanhToan - self.inforHoaDon.PhaiThanhToanBaoHiem;
            self.Caculator_ThucThu();
            self.UpdateChietKhauNV_ifChangeThucThu();
        },
        ResetAccountPOS: function () {
            var self = this;
            self.PhieuThuKhach.TenTaiKhoanPos = '';
            self.PhieuThuKhach.SoTaiKhoanPos = '';
            self.PhieuThuKhach.TenNganHangPos = '';
            self.PhieuThuKhach.ID_TaiKhoanPos = null;
            self.PhieuThuKhach.TienPOS = 0;
            self.CaculatorDaThanhToan();
        },
        ChangeAccountPOS: function (item) {
            var self = this;
            self.PhieuThuKhach.TenTaiKhoanPos = item.TenChuThe;
            self.PhieuThuKhach.SoTaiKhoanPos = item.SoTaiKhoan;
            self.PhieuThuKhach.TenNganHangPos = item.TenNganHang;
            self.PhieuThuKhach.ID_TaiKhoanPos = item.ID;
        },
        ChangeAccountCK: function (item) {
            var self = this;
            self.PhieuThuKhach.TenTaiKhoanCK = item.TenChuThe;
            self.PhieuThuKhach.SoTaiKhoanCK = item.SoTaiKhoan;
            self.PhieuThuKhach.TenNganHangCK = item.TenNganHang;
            self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan = item.ID;
            self.QRCode.TenNganHangCK = item.TenChuThe;
            self.QRCode.MaPin = item.MaPinNganHang;
            self.QRCode.SoTaiKhoan = item.SoTaiKhoan;
            if (typeof self.PhieuThuKhach.TienCK === 'number') {
                self.QRCode.SoTien = self.PhieuThuKhach.TienCK.toString();
            } else {
                self.QRCode.SoTien = self.PhieuThuKhach.TienCK.replace(/,/g, '');
            }

            console.log("change AccountCK", self.QRCode);
            self.updateQRCode();
        },
        ResetAccountCK: function () {
            var self = this;
            self.PhieuThuKhach.TenTaiKhoanCK = '';
            self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan = null;
            self.PhieuThuKhach.SoTaiKhoanCK = '';
            self.PhieuThuKhach.TenNganHangCK = '';
            self.PhieuThuKhach.TienCK = 0;
            self.CaculatorDaThanhToan();
            self.QRCode.SoTaiKhoan = '';
            self.QRCode.TenNganHangCK = '';
            self.QRCode.MaPinNganHang = '';
        },

        BH_ResetAccountPOS: function () {
            var self = this;
            self.PhieuThuBaoHiem.TenTaiKhoanPos = '';
            self.PhieuThuBaoHiem.ID_TaiKhoanPos = null;
            self.PhieuThuBaoHiem.TenNganHangPos = '';
            self.PhieuThuBaoHiem.SoTaiKhoanPos = null;
            self.PhieuThuBaoHiem.TienPOS = 0;
            self.CaculatorDaThanhToan();
        },
        BH_ChangeAccountPOS: function (item) {
            var self = this;
            self.PhieuThuBaoHiem.TenTaiKhoanPos = item.TenChuThe;
            self.PhieuThuBaoHiem.ID_TaiKhoanPos = item.ID;
            self.PhieuThuBaoHiem.SoTaiKhoanPos = item.SoTaiKhoan;
            self.PhieuThuBaoHiem.TenNganHangPos = item.TenNganHang;
        },
        BH_ChangeAccountCK: function (item) {
            var self = this;
            self.PhieuThuBaoHiem.TenTaiKhoanCK = item.TenChuThe;
            self.PhieuThuBaoHiem.ID_TaiKhoanChuyenKhoan = item.ID;
            self.PhieuThuBaoHiem.SoTaiKhoanCK = item.SoTaiKhoan;
            self.PhieuThuBaoHiem.TenNganHangCK = item.TenNganHang;
            self.QRCodeBH.MaNganHang = item.MaNganHang;
            self.QRCodeBH.SoTaiKhoan = item.SoTaiKhoan;
        },
        BH_ResetAccountCK: function () {
            var self = this;
            self.PhieuThuBaoHiem.TenTaiKhoanCK = '';
            self.PhieuThuBaoHiem.ID_TaiKhoanPos = null;
            self.PhieuThuBaoHiem.SoTaiKhoanCK = '';
            self.PhieuThuBaoHiem.TenNganHangCK = '';
            self.PhieuThuBaoHiem.TienCK = 0;
            self.BH_CaculatorDaThanhToan();
            self.QRCodeBH.MaNganHang = '';
            self.QRCodeBH.SoTaiKhoan = '';
        },
        Change_IsShareDiscount: function (x) {
            var self = this;
            self.HoaHongHD_UpdateHeSo_AndBind();
        },
        HoaHongHD_RemoveNhanVien: function (index) {
            var self = this;
            for (let i = 0; i < self.GridNVienBanGoi_Chosed.length; i++) {
                if (i === index) {
                    self.GridNVienBanGoi_Chosed.splice(i, 1);
                    break;
                }
            }
            self.HoaHongHD_UpdateHeSo_AndBind();
        },
        HoaHongHD_EditChietKhau: function () {
            var self = this;
            var item = self.itemChosing;
            var thisObj = event.currentTarget;
            var gtriNhap = formatNumberToFloat($(thisObj).val());
            var tinhCKTheo = parseInt(self.LoaiChietKhauHD_NV);
            var ptramCK = 0;
            if (tinhCKTheo === 3) {
                formatNumberObj($(thisObj))
            }
            else {
                if (gtriNhap > 100) {
                    $(thisObj).val(100);
                }
            }
            // get gtri after check % or VND
            var gtriCK_After = formatNumberToFloat($(thisObj).val());
            if (tinhCKTheo !== 3) {
                ptramCK = gtriCK_After;
            }
            var tienCK = self.CaculatorAgain_TienDuocNhan(gtriCK_After, item.HeSo, tinhCKTheo);
            for (let i = 0; i < self.GridNVienBanGoi_Chosed.length; i++) {
                if (i === item.Index) {
                    self.GridNVienBanGoi_Chosed[i].PT_ChietKhau = ptramCK;
                    self.GridNVienBanGoi_Chosed[i].TienChietKhau = tienCK;
                    self.GridNVienBanGoi_Chosed[i].ChietKhauMacDinh = gtriCK_After;
                    break;
                }
            }
        },
        AddNhanVien_BanGoi: function (item) {
            var self = this;
            var idNhanVien = item.ID;
            // check IDNhanVien exist in grid with same TacVu
            var itemEx = $.grep(self.GridNVienBanGoi_Chosed, function (x) {
                return x.ID_NhanVien === idNhanVien;
            });
            if (itemEx.length > 0) {
                ShowMessage_Danger('Nhân viên ' + itemEx[0].TenNhanVien + ' đã được chọn');
                return;
            }
            var loaiHD = self.inforHoaDon.LoaiHoaDon.toString();
            // get all ChietKhau HoaDon with LoaiHD
            var lstCK = $.grep(self.listData.ChietKhauHoaDons, function (x) {
                return x.ChungTuApDung.indexOf(loaiHD) > -1;
            })
            // remove ChungTu not apply LoaiHoaDon (ex: loaiHD= 1, but ChungTu contain 19
            var arrAfter = [];
            for (let i = 0; i < lstCK.length; i++) {
                var arrChungTu = lstCK[i].ChungTuApDung.split(',');
                if ($.inArray(loaiHD, arrChungTu) > -1) {
                    arrAfter.push(lstCK[i]);
                }
            }
            var exist = false;
            for (let i = 0; i < arrAfter.length; i++) {
                let itemOut = arrAfter[i];
                for (let j = 0; j < itemOut.NhanViens.length; j++) {
                    if (itemOut.NhanViens[j].ID === idNhanVien) {
                        let newObject1 = self.newNhanVien_ChietKhauHoaDon(itemOut, item, true);
                        self.GridNVienBanGoi_Chosed.unshift(newObject1);
                        exist = true;
                        break;
                    }
                }
            }
            if (exist === false) {
                let newObject2 = self.newNhanVien_ChietKhauHoaDon(null, item, false);
                self.GridNVienBanGoi_Chosed.push(newObject2);
            }
            self.HoaHongHD_UpdateHeSo_AndBind();
        },
        UpdateChietKhauNV_ifChangeThucThu: function () {
            var self = this;
            var thucthu = self.PhieuThuKhach.ThucThu + self.PhieuThuBaoHiem.ThucThu;
            for (let i = 0; i < self.GridNVienBanGoi_Chosed.length; i++) {
                let itemFor = self.GridNVienBanGoi_Chosed[i];
                if (parseInt(itemFor.TinhChietKhauTheo) === 1) {
                    self.GridNVienBanGoi_Chosed[i].TienChietKhau = thucthu * (itemFor.PT_ChietKhau / 100) * itemFor.HeSo;
                }
            }
        },
        ChangeCheckTraLaiCoc: function () {
            let self = this;
            if (self.isCheckTraLaiCoc) {
                self.PhieuThuKhach.TienMat = formatNumber3Digit(self.PhieuThuKhach.HoanTraTamUng);
            }
            else {
                self.PhieuThuKhach.TienMat = 0;
            }
            self.PhieuThuKhach.TienPOS = 0;
            self.PhieuThuKhach.TienCK = 0;
            self.CaculatorDaThanhToan();
        },

        KH_EditTienCoc: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            var tienmat = 0;
            // !!important: get self.inforHoaDon.PhaiThanhToan
            var khachcantra = formatNumberToFloat(self.inforHoaDon.PhaiThanhToan) - self.inforHoaDon.KhachDaTra;
            var soduDatCoc = self.inforHoaDon.SoDuDatCoc;
            var datcoc = formatNumberToFloat($this.val());
            if (datcoc >= khachcantra) {
                if (datcoc >= soduDatCoc) {
                    if (soduDatCoc > khachcantra) {
                        datcoc = khachcantra;
                        tienmat = soduDatCoc - datcoc;
                    }
                    else {
                        datcoc = soduDatCoc;
                        tienmat = khachcantra - datcoc;
                    }
                }
                else {
                    datcoc = khachcantra;
                    tienmat = soduDatCoc - datcoc;
                }
            }
            else {
                if (datcoc >= soduDatCoc) {
                    datcoc = soduDatCoc;
                }
                tienmat = khachcantra - datcoc;
            }
            self.PhieuThuKhach.TienDatCoc = formatNumber3Digit(datcoc, 2);

            var hoantraOld = self.inforHoaDon.HoanTraTamUng;
            if (hoantraOld > 0 && soduDatCoc > 0) {
                if (datcoc < khachcantra && datcoc < soduDatCoc) {
                    self.PhieuThuKhach.HoanTraTamUng = 0;
                    self.isCheckTraLaiCoc = false;
                }
                else {
                    self.PhieuThuKhach.HoanTraTamUng = datcoc;
                }
            }

            self.PhieuThuKhach.TienMat = formatNumber3Digit(tienmat, 2);
            self.PhieuThuKhach.PhaiThanhToan = tienmat;

            self.PhieuThuKhach.TienPOS = 0;
            self.PhieuThuKhach.TienCK = 0;
            self.PhieuThuKhach.TienTheGiaTri = 0;
            self.CaculatorDaThanhToan();

            var key = event.keyCode || event.which;
            if (key === 13) {
                $this.parent().next().find('input').focus();
            }
        },

        KH_EditTienMat: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            var tienmat = formatNumberToFloat($this.val());
            self.PhieuThuKhach.TienMat = $this.val();

            var tienthe = formatNumberToFloat(self.PhieuThuKhach.TienTheGiaTri);
            var tienpos = 0, tienck = 0;
            var cantt = self.PhieuThuKhach.PhaiThanhToan - tienmat - self.PhieuThuKhach.TTBangDiem - tienthe;
            if (self.PhieuThuKhach.ID_TaiKhoanPos !== null) {
                tienpos = cantt;
                tienpos = tienpos > 0 ? tienpos : 0;
                tienck = 0;
            }
            else {
                if (self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan !== null) {
                    tienck = cantt;
                    tienck = tienck > 0 ? tienck : 0;
                }
            }
            self.PhieuThuKhach.TienPOS = formatNumber3Digit(tienpos, 2);
            self.PhieuThuKhach.TienCK = formatNumber3Digit(tienck, 2);
            self.CaculatorDaThanhToan();

            // Cập nhật số tiền trong QRCode (trong trường hợp tiền CK)
            self.QRCode.SoTien = self.PhieuThuKhach.TienCK.replace(/,/g, '');
            self.updateQRCode();

            var key = event.keyCode || event.which;
            if (key === 13) {
                if (self.PhieuThuKhach.ID_TaiKhoanPos !== null) {
                    $this.closest('.container-fluid').next().find('input').select();
                }
                else {
                    if (self.inforHoaDon.LoaiHoaDon === 6) {
                        if (self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan !== null) {
                            $this.closest('.container-fluid').next().find('input').select();
                        }
                    }
                    else {
                        if (self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan !== null) {
                            $this.closest('.container-fluid').next().next().find('input').select();
                        }
                    }
                }
            }
        },
        KH_EditTienPos: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            var tienpos = formatNumberToFloat($this.val());
            self.PhieuThuKhach.TienPOS = $this.val();

            var tienck = 0;
            var tienmat = formatNumberToFloat(self.PhieuThuKhach.TienMat);
            var tienthe = formatNumberToFloat(self.PhieuThuKhach.TienTheGiaTri);
            if (self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan !== null) {
                tienck = self.PhieuThuKhach.PhaiThanhToan - tienmat - tienpos - self.PhieuThuKhach.TTBangDiem - tienthe;
                tienck = tienck < 0 ? 0 : tienck;
            }
            self.PhieuThuKhach.TienCK = formatNumber3Digit(tienck, 2);
            self.CaculatorDaThanhToan();

            // Cập nhật số tiền trong QRCode (trong trường hợp tiền CK)
            self.QRCode.SoTien = self.PhieuThuKhach.TienCK.replace(/,/g, '');
            self.updateQRCode();

            var key = event.keyCode || event.which;
            if (key === 13) {
                if (self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan !== null) {
                    $this.closest('.container-fluid').next().find('input').select();
                }
                else {
                    if (self.theGiaTriCus.SoDuTheGiaTri > 0) {
                        $this.closest('.container-fluid').next().next().find('input').select();
                    }
                }
            }
        },
        KH_EditTienCK: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            self.PhieuThuKhach.TienCK = $this.val();
            self.CaculatorDaThanhToan();
            // Cập nhật số tiền trong QRCode
            self.QRCode.SoTien = self.PhieuThuKhach.TienCK.replace(/,/g, '');
            self.updateQRCode();
        },
        KH_EditTienThe: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);
            if (formatNumberToFloat($this.val()) > self.theGiaTriCus.SoDuTheGiaTri) {
                ShowMessage_Danger('Nhập quá số dư thẻ');
                $this.val(formatNumber3Digit(self.theGiaTriCus.SoDuTheGiaTri));
                self.PhieuThuKhach.TienTheGiaTri = formatNumber3Digit(self.theGiaTriCus.SoDuTheGiaTri, 2);
                self.CaculatorDaThanhToan();
                return;
            }
            self.PhieuThuKhach.TienTheGiaTri = $this.val();

            var tienmat = self.PhieuThuKhach.PhaiThanhToan - formatNumberToFloat($this.val()) - self.PhieuThuKhach.TTBangDiem;
            tienmat = tienmat < 0 ? 0 : tienmat;
            self.PhieuThuKhach.TienMat = formatNumber3Digit(tienmat, 2);
            self.CaculatorDaThanhToan();

            var key = event.keyCode || event.which;
            if (key === 13) {
                $this.closest('.container-fluid').next().find('input').select();
            }
        },

        KH_HoanTraTienThe: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            self.PhieuThuKhach.TienTheGiaTri = $this.val();
            self.CaculatorDaThanhToan();
        },

        KH_TTBangDiem: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            var diemQD = formatNumberToFloat($this.val());
            if (!self.ThietLap_TichDiem.DuocThietLap) {
                return;
            }
            if (self.inforHoaDon.TongTichDiem < diemQD) {
                ShowMessage_Danger('Vượt quá số điểm hiện tại ');
                return;
            }
            var diemTT = self.ThietLap_TichDiem.DiemThanhToan;
            diemTT = diemTT === 0 ? 1 : diemTT;
            var tienQuyDoi = Math.floor(diemQD * self.ThietLap_TichDiem.TienThanhToan / self.ThietLap_TichDiem.DiemThanhToan);
            self.PhieuThuKhach.DiemQuyDoi = $this.val();
            self.PhieuThuKhach.TTBangDiem = tienQuyDoi;

            // caculator again tienmat
            var tienMat = self.PhieuThuKhach.PhaiThanhToan - tienQuyDoi - formatNumberToFloat(self.PhieuThuKhach.TienTheGiaTri);
            tienMat = tienMat < 0 ? 0 : tienMat;
            self.PhieuThuKhach.TienMat = formatNumber3Digit(tienMat, 2);
            self.CaculatorDaThanhToan();
        },

        BH_EditTienMat: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            var tienmat = formatNumberToFloat($this.val());
            self.PhieuThuBaoHiem.TienMat = formatNumber3Digit(tienmat, 2);

            var tienpos = 0, tienck = 0;
            if (self.PhieuThuBaoHiem.ID_TaiKhoanPos !== null) {
                tienpos = self.inforHoaDon.PhaiThanhToanBaoHiem - tienmat;
                tienck = 0;
            }
            else {
                if (self.PhieuThuBaoHiem.ID_TaiKhoanChuyenKhoan !== null) {
                    tienck = self.inforHoaDon.PhaiThanhToanBaoHiem - tienmat;
                }
                else {

                }
            }
            self.PhieuThuBaoHiem.TienPOS = formatNumber3Digit(tienpos, 2);
            self.PhieuThuBaoHiem.TienCK = formatNumber3Digit(tienck, 2);
            self.BH_CaculatorDaThanhToan();

            var key = event.keyCode || event.which;
            if (key === 13) {
                if (self.PhieuThuBaoHiem.ID_TaiKhoanPos !== null) {
                    $this.parent().next().find('input').focus();
                }
                else {
                    if (self.PhieuThuBaoHiem.ID_TaiKhoanChuyenKhoan !== null) {
                        $this.parent().next().find('input').focus();
                    }
                    else {

                    }
                }
            }
        },
        BH_EditTienPos: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            var tienpos = formatNumberToFloat($this.val());
            self.PhieuThuBaoHiem.TienPOS = formatNumber3Digit(tienpos, 2);

            var tienck = 0;
            var tienmat = formatNumberToFloat(self.PhieuThuBaoHiem.TienMat);
            if (self.PhieuThuBaoHiem.ID_TaiKhoanChuyenKhoan !== null) {
                tienck = self.inforHoaDon.PhaiThanhToanBaoHiem - tienmat - tienpos;
            }
            self.PhieuThuBaoHiem.TienCK = formatNumber3Digit(tienck, 2);
            self.BH_CaculatorDaThanhToan();

            var key = event.keyCode || event.which;
            if (key === 13) {
                if (self.PhieuThuBaoHiem.ID_TaiKhoanChuyenKhoan !== null) {
                    $this.parent().next().find('input').focus();
                }
                else {
                    if (self.PhieuThuBaoHiem.SoDuTheGiaTri > 0) {
                        $this.parent().next().next().find('input').focus();
                    }
                }
            }
        },
        BH_EditTienCK: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            self.PhieuThuBaoHiem.TienCK = $this.val();
            self.BH_CaculatorDaThanhToan();

            var key = event.keyCode || event.which;
            if (key === 13) {
                if (self.PhieuThuBaoHiem.SoDuTheGiaTri > 0) {
                    $this.parent().next().find('input').focus();
                }
            }
        },
        BH_EditTienThe: function () {
            var self = this;
            var $this = $(event.currentTarget);
            formatNumberObj($this);

            self.PhieuThuBaoHiem.TienTheGiaTri = formatNumberToFloat($this.val());
            self.BH_CaculatorDaThanhToan();

            var key = event.keyCode || event.which;
            if (key === 13) {
                if (self.PhieuThuKhach.ID_TaiKhoanPos !== null) {
                    $this.parent().next().find('input').focus();
                }
                else {
                    if (self.PhieuThuKhach.ID_TaiKhoanChuyenKhoan !== null) {
                        $this.parent().next().find('input').focus();
                    }
                    else {

                    }
                }
            }
        },
        Caculator_ThucThu: function () {
            var self = this;
            var kh_tiencoc = formatNumberToFloat(self.PhieuThuKhach.TienDatCoc);
            var kh_tienmat = formatNumberToFloat(self.PhieuThuKhach.TienMat);
            var kh_tienpos = formatNumberToFloat(self.PhieuThuKhach.TienPOS);
            var kh_tienck = formatNumberToFloat(self.PhieuThuKhach.TienCK);
            var kh_tienthe = formatNumberToFloat(self.PhieuThuKhach.TienTheGiaTri);
            var kh_tiendiem = formatNumberToFloat(self.PhieuThuKhach.TTBangDiem);
            var kh_tienthua = self.PhieuThuKhach.TienThua;
            kh_tienthua = kh_tienthua > 0 ? kh_tienthua : 0;

            var bh_tienmat = formatNumberToFloat(self.PhieuThuBaoHiem.TienMat);
            var bh_tienpos = formatNumberToFloat(self.PhieuThuBaoHiem.TienPOS);
            var bh_tienck = formatNumberToFloat(self.PhieuThuBaoHiem.TienCK);
            var bh_tienthe = formatNumberToFloat(self.PhieuThuBaoHiem.TienTheGiaTri);
            var bh_tienthua = self.PhieuThuBaoHiem.TienThua;
            bh_tienthua = bh_tienthua > 0 ? bh_tienthua : 0;

            var khachtt = kh_tiencoc + kh_tienmat + kh_tienpos + kh_tienck + kh_tienthe + kh_tiendiem;
            var baohiemtt = bh_tienmat + bh_tienpos + bh_tienck + bh_tienthe;
            var thucthuHD = 0;
            if (self.PhieuThuKhach.HoanTraTamUng > 0) {
                thucthuHD = kh_tiencoc + baohiemtt - bh_tienthua;
            }
            else {
                thucthuHD = khachtt + baohiemtt - kh_tienthua - bh_tienthua;
            }
            self.inforHoaDon.ThucThu = thucthuHD;
            if (self.inforHoaDon.LoaiHoaDon === 6) {
                self.inforHoaDon.ConNo = self.inforHoaDon.PhaiThanhToan - thucthuHD;
            }
            else {
                self.inforHoaDon.ConNo = self.inforHoaDon.TongThanhToan - thucthuHD - self.inforHoaDon.KhachDaTra;
            }
            self.UpdateChietKhauNV_ifChangeThucThu();
        },

        HoaHongHD_UpdateHeSo_AndBind: function () {
            var self = this;
            var heso = 1;
            var lenGrid = self.GridNVienBanGoi_Chosed.length;
            if (self.IsShareDiscount === '1') {
                heso = RoundDecimal(1 / lenGrid, 2);
            }
            var thucthu = formatNumberToFloat(self.PhieuThuKhach.ThucThu) + formatNumberToFloat(self.PhieuThuBaoHiem.ThucThu);
            var doanhthu = formatNumberToFloat(self.inforHoaDon.TongThanhToan) - self.inforHoaDon.TongTienThue;
            for (let i = 0; i < self.GridNVienBanGoi_Chosed.length; i++) {
                let itemFor = self.GridNVienBanGoi_Chosed[i];
                let tinhCKTheo = parseInt(itemFor.TinhChietKhauTheo);
                let ptCK = itemFor.PT_ChietKhau;
                let tienCK = itemFor.TienChietKhau;
                switch (tinhCKTheo) {
                    case 1:
                        tienCK = Math.round(thucthu * ptCK / 100 * heso);
                        break;
                    case 2:
                        tienCK = Math.round(doanhthu * ptCK / 100 * heso);
                        break;
                    case 3:// vnd, keep heso =1
                        if (heso !== 1) {
                            tienCK = itemFor.ChietKhauMacDinh * heso;
                        }
                        else {
                            tienCK = itemFor.ChietKhauMacDinh / heso;
                        }
                        break;
                }
                self.GridNVienBanGoi_Chosed[i].HeSo = heso;
                self.GridNVienBanGoi_Chosed[i].TienChietKhau = tienCK;
            }
        },

        HoaHongHD_ShowDivChietKhau: function (item, index) {
            var self = this;
            var thisObj = $(event.currentTarget);
            if (self.RoleChange_ChietKhauNV === false) {
                ShowMessage_Danger('Không có quyền thay đổi chiết khấu nhân viên');
                return false;
            }
            var pos = thisObj.closest('td').position();
            $('#jsDiscountKH').show();
            $('#jsDiscountKH').css({
                left: (pos.left - 120) + "px",
                top: (pos.top + 71) + "px"
            });

            item.Index = index;
            self.itemChosing = item;
            var tinhCKTheo = parseInt(item.TinhChietKhauTheo);
            var gtriCK = 0;
            switch (tinhCKTheo) {
                case 1:
                    gtriCK = item.ChietKhauMacDinh;
                    break;
                case 2:
                    gtriCK = item.ChietKhauMacDinh;
                    break;
                case 3:
                    gtriCK = formatNumber3Digit(item.ChietKhauMacDinh, 2);
                    break;
            }
            self.LoaiChietKhauHD_NV = tinhCKTheo.toString();
            $(function () {
                let inputNext = $('#jsDiscountKH').find("input[type!='radio']").eq(0);
                $(inputNext).val(gtriCK);
                $(inputNext).focus().select();
            });
        },
        CaculatorAgain_TienDuocNhan: function (gtriCK, heso, tinhCKTheo) {
            var self = this;
            var doanhthu = self.inforHoaDon.TongThanhToan - self.inforHoaDon.TongTienThue;
            var thucthu = self.PhieuThuKhach.ThucThu + self.PhieuThuBaoHiem.ThucThu;
            var tienCK = 0;
            switch (parseInt(tinhCKTheo)) {
                case 1:
                    tienCK = Math.round(thucthu * gtriCK / 100 * heso);
                    break;
                case 2:
                    tienCK = Math.round(doanhthu * gtriCK / 100 * heso);
                    break;
                case 3:
                    if (heso !== 1) {
                        tienCK = gtriCK * heso;
                    }
                    else {
                        tienCK = gtriCK / heso;
                    }
                    break;
            }
            return tienCK;
        },

        HoaHongHD_ChangeLoaiChietKhau: function (loaiCK) {
            var self = this;
            var item = self.itemChosing;
            var gtriCK = item.ChietKhauMacDinh;
            var ptramCK = gtriCK;
            var chietKhauMacDinh = 0;
            var doanhthu = formatNumberToFloat(self.inforHoaDon.TongThanhToan) - self.inforHoaDon.TongTienThue;
            var thucthu = self.PhieuThuKhach.ThucThu + self.PhieuThuBaoHiem.ThucThu;
            var loaiCK_Old = parseInt(self.LoaiChietKhauHD_NV);
            if (loaiCK_Old === 3) {
                switch (loaiCK) {
                    case 1:// thuc thu
                        ptramCK = gtriCK = RoundDecimal(gtriCK / thucthu * 100);
                        chietKhauMacDinh = ptramCK;
                        break;
                    case 2:// doanh thu
                        ptramCK = gtriCK = RoundDecimal(gtriCK / doanhthu * 100);
                        chietKhauMacDinh = ptramCK;
                        break;
                    case 3:
                        ptramCK = 0;
                        break;
                }
            }
            else {
                switch (loaiCK) {
                    case 3:
                        if (loaiCK_Old === 1) {
                            gtriCK = Math.round(ptramCK * thucthu) / 100;
                        }
                        if (loaiCK_Old === 2) {
                            gtriCK = Math.round(ptramCK * doanhthu) / 100;
                        }
                        ptramCK = 0;
                        chietKhauMacDinh = gtriCK;
                        break;
                }
            }
            var tienCK = self.CaculatorAgain_TienDuocNhan(gtriCK, item.HeSo, loaiCK);
            for (let i = 0; i < self.GridNVienBanGoi_Chosed.length; i++) {
                if (i === item.Index) {
                    self.GridNVienBanGoi_Chosed[i].TinhChietKhauTheo = loaiCK.toString();
                    self.GridNVienBanGoi_Chosed[i].PT_ChietKhau = ptramCK;
                    self.GridNVienBanGoi_Chosed[i].TienChietKhau = tienCK;
                    if (chietKhauMacDinh !== 0 || (chietKhauMacDinh === 0 && tienCK === 0)) {
                        self.GridNVienBanGoi_Chosed[i].ChietKhauMacDinh = chietKhauMacDinh;
                    }
                    break;
                }
            }
            self.HoaHongHD_UpdateHeSo_AndBind();
            $(event.currentTarget).closest('div').prev().find('input').select().focus();
        },
        HoaHongHD_EditHeSo: function (item, index) {
            var self = this;
            var thisObj = event.currentTarget;
            var gtriCK = item.ChietKhauMacDinh;
            var heso = formatNumberToFloat($(thisObj).val());
            var tienCK = self.CaculatorAgain_TienDuocNhan(gtriCK, heso, item.TinhChietKhauTheo);
            for (let i = 0; i < self.GridNVienBanGoi_Chosed.length; i++) {
                if (index === i) {
                    self.GridNVienBanGoi_Chosed[i].TienChietKhau = tienCK;
                    break;
                }
            }
        },

        shareMoney_QuyHD: function (phaiTT, tienDiem, tienmat, tienPOS, chuyenkhoan, thegiatri, tiencoc) {
            // thutu uutien: 1.coc, 2.diem, 3.thegiatri, 4.pos, 5.chuyenkhoan, mat
            if (tiencoc >= phaiTT) {
                return {
                    TienCoc: phaiTT,
                    TTBangDiem: 0,
                    TienMat: 0,
                    TienPOS: 0,
                    TienChuyenKhoan: 0,
                    TienTheGiaTri: 0,
                }
            }
            else {
                phaiTT = phaiTT - tiencoc;
                if (tienDiem >= phaiTT) {
                    return {
                        TienCoc: tiencoc,
                        TTBangDiem: phaiTT,
                        TienMat: 0,
                        TienPOS: 0,
                        TienChuyenKhoan: 0,
                        TienTheGiaTri: 0,
                    }
                }
                else {
                    phaiTT = phaiTT - tienDiem;
                    if (thegiatri >= phaiTT) {
                        return {
                            TienCoc: tiencoc,
                            TTBangDiem: tienDiem,
                            TienMat: 0,
                            TienPOS: 0,
                            TienChuyenKhoan: 0,
                            TienTheGiaTri: Math.abs(phaiTT),
                        }
                    }
                    else {
                        phaiTT = phaiTT - thegiatri;
                        if (tienPOS >= phaiTT) {
                            return {
                                TienCoc: tiencoc,
                                TTBangDiem: tienDiem,
                                TienMat: 0,
                                TienPOS: Math.abs(phaiTT),
                                TienChuyenKhoan: 0,
                                TienTheGiaTri: thegiatri,
                            }
                        }
                        else {
                            phaiTT = phaiTT - tienPOS;
                            if (chuyenkhoan >= phaiTT) {
                                return {
                                    TienCoc: tiencoc,
                                    TTBangDiem: tienDiem,
                                    TienMat: 0,
                                    TienPOS: tienPOS,
                                    TienChuyenKhoan: Math.abs(phaiTT),
                                    TienTheGiaTri: thegiatri,
                                }
                            }
                            else {
                                phaiTT = phaiTT - chuyenkhoan;
                                if (tienmat >= phaiTT) {
                                    return {
                                        TienCoc: tiencoc,
                                        TTBangDiem: tienDiem,
                                        TienMat: Math.abs(phaiTT),
                                        TienPOS: tienPOS,
                                        TienChuyenKhoan: chuyenkhoan,
                                        TienTheGiaTri: thegiatri,
                                    }
                                }
                                else {
                                    phaiTT = phaiTT - tienmat;
                                    return {
                                        TienCoc: tiencoc,
                                        TTBangDiem: tienDiem,
                                        TienMat: tienmat,
                                        TienPOS: tienPOS,
                                        TienChuyenKhoan: chuyenkhoan,
                                        TienTheGiaTri: thegiatri,
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        UpdateIDQuyHoaDon_toBHThucHien: async function (idHoaDon, idQuyHD) {
            var self = this;
            if (self.GridNVienBanGoi_Chosed.length > 0 && idHoaDon !== null) {
                for (let i = 0; i < self.GridNVienBanGoi_Chosed.length; i++) {
                    self.GridNVienBanGoi_Chosed[i].ID_QuyHoaDon = idQuyHD;
                    self.GridNVienBanGoi_Chosed[i].ID_HoaDon = idHoaDon;
                }

                let myData = {
                    lstObj: self.GridNVienBanGoi_Chosed
                }
                const xx = await ajaxHelper('/api/DanhMuc/BH_HoaDonAPI/' + 'Post_BHNhanVienThucHien', 'POST', myData).done()
                    .then(function (x) {
                        return x;
                    })
                self.GridNVienBanGoi_Chosed = [];
            }
            else {
                if (self.inforHoaDon.LoaiHoaDon === 6) {
                    const xx = await ajaxHelper('/api/DanhMuc/BH_HoaDonAPI/InsertChietKhauTraHang_TheoThucThu?idHoaDonTra=' + idHoaDon
                        + '&idPhieuChi=' + idQuyHD, 'GET').done()
                        .then(function (x) {
                            return x;
                        })
                    self.GridNVienBanGoi_Chosed = [];
                }
            }
        },

        AgreeThanhToan: function () {
            var self = this;
            self.saveOK = true;
            if (self.isLoading) {
                return;
            }
            switch (self.formType) {
                case 1:// gara
                    self.isLoading = true;
                    newModelBanLe.saveHoaDonTraHang();
                    break;
                default:
                    break;
            }
            $('#ThongTinThanhToanKHNCC').modal('hide');
        },

        SavePhieuThu_HDDoiTra: function () {
            var self = this;
            var ptKhach = self.PhieuThuKhach;

            if (ptKhach.DaThanhToan === 0 || ptKhach.DaThanhToan === undefined) {
                // if click btnTrahang - dont' save phieuthu
                return;
            }

            var hd = self.inforHoaDon;
            var ghichu = hd.DienGiai;
            var idHoaDon = hd.ID;
            var idDoiTuong = hd.ID_DoiTuong;
            var tenDoiTuong = hd.TenDoiTuong;
            var idKhoanThuChi = ptKhach.ID_KhoanThuChi;
            var sLoai = 'thu ';
            var maPhieuThuChi = 'TT' + hd.MaHoaDon;
            var chitracoc = self.isCheckTraLaiCoc;
            var tiendatcoc = 0, tienmat = 0, tienck = 0, tienthe = 0, tiendiem = 0, tongthu = 0;
            var lstQuyCT = [], lstQuyCT2 = [];
            var myData = {}, myData2 = {};
            var phuongthucTT = '', phuongthucTT2 = '';

            var chenhLechTraMua = hd.ChenhLechTraMua;
            if (chenhLechTraMua > 0) {//tra > mua
                //Cần tạo 2 phiếu chi: 
                //      +) phiếu chi 1: (ID_LienQuan là hóa đơn trả, HinhThucThanhToan = 1/hoac 3, TongThu = chenhlech tramua
                //      +) phiếu chi 2: chi trả cọc 1 triệu(ID_LienQuan = null, LoaiThanhToan = 1), TongChi = soducoc hientai

                sLoai = 'chi ';
                if (ptKhach.DaThanhToan > 0) {
                    let dataReturn = self.shareMoney_QuyHD(ptKhach.PhaiThanhToan, ptKhach.TTBangDiem,
                        formatNumberToFloat(ptKhach.TienMat), formatNumberToFloat(ptKhach.TienPOS),
                        formatNumberToFloat(ptKhach.TienCK), formatNumberToFloat(ptKhach.TienTheGiaTri),
                        0);

                    tiendatcoc = dataReturn.TienCoc;
                    tienmat = dataReturn.TienMat;
                    tienpos = dataReturn.TienPOS;
                    tienck = dataReturn.TienChuyenKhoan;
                    tienthe = dataReturn.TienTheGiaTri;
                    tiendiem = dataReturn.TTBangDiem;
                    tongthu = tienmat + tienpos + tienck + tienthe + tiendiem + tiendatcoc;

                    let conlai = 0;

                    if (tienck > 0) {
                        if (tienck > chenhLechTraMua) {
                            conlai = tienck - chenhLechTraMua;

                            // phieuchi1: ck all
                            let qct = newQuyChiTiet({
                                ID_HoaDonLienQuan: idHoaDon,
                                ID_KhoanThuChi: idKhoanThuChi,
                                ID_DoiTuong: idDoiTuong,
                                GhiChu: ghichu,
                                TienThu: chenhLechTraMua,
                                TienChuyenKhoan: chenhLechTraMua,
                                HinhThucThanhToan: 3,
                                ID_TaiKhoanNganHang: ptKhach.ID_TaiKhoanChuyenKhoan,
                                LoaiThanhToan: 0,
                            });
                            phuongthucTT += 'Chuyển khoản, ';

                            let nowSeconds = (new Date()).getSeconds() + 1;
                            let quyhd = {
                                LoaiHoaDon: 12,
                                TongTienThu: chenhLechTraMua,
                                MaHoaDon: maPhieuThuChi,
                                NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                                NguoiNopTien: tenDoiTuong,
                                NguoiTao: hd.NguoiTao,
                                NoiDungThu: ghichu,
                                ID_NhanVien: hd.ID_NhanVien,
                                ID_DonVi: hd.ID_DonVi,
                                // used to get save diary 
                                MaHoaDonTraHang: hd.MaHoaDon,
                                ID_DoiTuong: idDoiTuong,
                            }

                            myData = {
                                objQuyHoaDon: quyhd,
                                lstCTQuyHoaDon: [qct]
                            }

                            // phieuchi2: ck 1phan
                            let qct2 = newQuyChiTiet({
                                ID_HoaDonLienQuan: null,
                                ID_KhoanThuChi: idKhoanThuChi,
                                ID_DoiTuong: idDoiTuong,
                                GhiChu: ghichu,
                                TienThu: conlai,
                                TienChuyenKhoan: conlai,
                                HinhThucThanhToan: 3,
                                ID_TaiKhoanNganHang: ptKhach.ID_TaiKhoanChuyenKhoan,
                                LoaiThanhToan: chitracoc ? 1 : 0,
                            });
                            lstQuyCT2.push(qct2);
                            phuongthucTT2 += 'Chuyển khoản, ';
                        }
                        else {
                            // phieuchi1: ck 1phan
                            let qct = newQuyChiTiet({
                                ID_HoaDonLienQuan: idHoaDon,
                                ID_KhoanThuChi: idKhoanThuChi,
                                ID_DoiTuong: idDoiTuong,
                                GhiChu: ghichu,
                                TienThu: tienck,
                                TienChuyenKhoan: tienck,
                                HinhThucThanhToan: 3,
                                ID_TaiKhoanNganHang: ptKhach.ID_TaiKhoanChuyenKhoan,
                                LoaiThanhToan: 0,
                            });
                            lstQuyCT.push(qct);
                            phuongthucTT += 'Chuyển khoản, ';

                        }
                    }

                    if (tienmat > 0) {
                        if (tienmat > chenhLechTraMua) {
                            let tienmatDaDung = 0;
                            if (lstQuyCT.length > 0) {// da tt 1 phan ck
                                let thuMatCL = chenhLechTraMua - lstQuyCT[0].TienThu;;
                                let qct = newQuyChiTiet({
                                    ID_HoaDonLienQuan: idHoaDon,
                                    ID_KhoanThuChi: idKhoanThuChi,
                                    ID_DoiTuong: idDoiTuong,
                                    GhiChu: ghichu,
                                    TienThu: thuMatCL,
                                    TienMat: thuMatCL,
                                    HinhThucThanhToan: 1,
                                    LoaiThanhToan: 0,
                                });
                                lstQuyCT.push(qct);
                                phuongthucTT += 'Tiền mặt, ';
                                tienmatDaDung = thuMatCL;
                            }
                            else {
                                tienmatDaDung = chenhLechTraMua;
                                // phieuchi1: mat all
                                let qct = newQuyChiTiet({
                                    ID_HoaDonLienQuan: idHoaDon,
                                    ID_KhoanThuChi: idKhoanThuChi,
                                    ID_DoiTuong: idDoiTuong,
                                    GhiChu: ghichu,
                                    TienThu: chenhLechTraMua,
                                    TienMat: chenhLechTraMua,
                                    HinhThucThanhToan: 1,
                                    LoaiThanhToan: 0,
                                });
                                phuongthucTT += 'Tiền mặt, ';

                                let nowSeconds = (new Date()).getSeconds() + 1;
                                let quyhd = {
                                    LoaiHoaDon: 12,
                                    TongTienThu: chenhLechTraMua,
                                    MaHoaDon: maPhieuThuChi,
                                    NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                                    NguoiNopTien: tenDoiTuong,
                                    NguoiTao: hd.NguoiTao,
                                    NoiDungThu: ghichu,
                                    ID_NhanVien: hd.ID_NhanVien,
                                    ID_DonVi: hd.ID_DonVi,
                                    // used to get save diary 
                                    MaHoaDonTraHang: hd.MaHoaDon,
                                    ID_DoiTuong: idDoiTuong,
                                }

                                myData = {
                                    objQuyHoaDon: quyhd,
                                    lstCTQuyHoaDon: [qct]
                                }
                            }

                            // phieuchi2: mat 1phan
                            conlai = tienmat - tienmatDaDung;
                            let qct2 = newQuyChiTiet({
                                ID_HoaDonLienQuan: null,
                                ID_KhoanThuChi: idKhoanThuChi,
                                ID_DoiTuong: idDoiTuong,
                                GhiChu: ghichu,
                                TienThu: conlai,
                                TienMat: conlai,
                                HinhThucThanhToan: 1,
                                LoaiThanhToan: chitracoc ? 1 : 0,
                            });
                            lstQuyCT2.push(qct2);
                            phuongthucTT2 += 'Tiền mặt, ';
                        }
                        else {
                            // phieuchi1: mat 1phan
                            let qct = newQuyChiTiet({
                                ID_HoaDonLienQuan: idHoaDon,
                                ID_KhoanThuChi: idKhoanThuChi,
                                ID_DoiTuong: idDoiTuong,
                                GhiChu: ghichu,
                                TienThu: tienmat,
                                TienMat: tienmat,
                                HinhThucThanhToan: 1,
                                LoaiThanhToan: 0,
                            });
                            lstQuyCT.push(qct);
                            phuongthucTT += 'Tiền mặt, ';
                        }
                    }

                    if (lstQuyCT.length > 0) {

                        let tongThu1 = 0;
                        for (let i = 0; i < lstQuyCT.length; i++) {
                            tongThu1 += lstQuyCT[i].TienThu;
                        }
                        let nowSeconds = (new Date()).getSeconds() + 1;
                        let quyhd = {
                            LoaiHoaDon: 12,
                            TongTienThu: tongThu1,
                            MaHoaDon: maPhieuThuChi,
                            NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                            NguoiNopTien: tenDoiTuong,
                            NguoiTao: hd.NguoiTao,
                            NoiDungThu: ghichu,
                            ID_NhanVien: hd.ID_NhanVien,
                            ID_DonVi: hd.ID_DonVi,
                            // used to get save diary 
                            MaHoaDonTraHang: hd.MaHoaDon,
                            ID_DoiTuong: idDoiTuong,
                        }
                        myData = {
                            objQuyHoaDon: quyhd,
                            lstCTQuyHoaDon: lstQuyCT
                        }
                    }

                    if (lstQuyCT2.length > 0) {

                        let tongThu2 = 0;
                        for (let i = 0; i < lstQuyCT2.length; i++) {
                            tongThu2 += lstQuyCT2[i].TienThu;
                        }
                        let nowSeconds = (new Date()).getSeconds() + 1;
                        let quyhd = {
                            LoaiHoaDon: 12,
                            TongTienThu: tongThu2,
                            MaHoaDon: maPhieuThuChi + '_2',
                            NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                            NguoiNopTien: tenDoiTuong,
                            NguoiTao: hd.NguoiTao,
                            NoiDungThu: ghichu,
                            ID_NhanVien: hd.ID_NhanVien,
                            ID_DonVi: hd.ID_DonVi,
                            // used to get save diary 
                            MaHoaDonTraHang: hd.MaHoaDon,
                            ID_DoiTuong: idDoiTuong,
                        }
                        myData2 = {
                            objQuyHoaDon: quyhd,
                            lstCTQuyHoaDon: lstQuyCT2
                        }
                    }
                }
            }
            else {
                chenhLechTraMua = Math.abs(chenhLechTraMua);
                // mua > tra
                //Cần tạo 1 phieuthu + 1 phieuchi
                //      +) phiếu thu: (ID_LienQuan là hóa đơn mua, HinhThucThanhToan 6)
                //      +) phiếu chi: ID_LienQuan = null, LoaiThanhToan = 1

                // always taophieuthu (thutucoc)
                let qct = newQuyChiTiet({
                    ID_HoaDonLienQuan: idHoaDon,
                    ID_KhoanThuChi: idKhoanThuChi,
                    ID_DoiTuong: idDoiTuong,
                    GhiChu: ghichu,
                    TienThu: chenhLechTraMua,
                    TienCoc: chenhLechTraMua,
                    HinhThucThanhToan: 6,
                });
                phuongthucTT = 'Thu từ cọc';
                let nowSeconds = (new Date()).getSeconds() + 1;
                let quyhd = {
                    LoaiHoaDon: 11,
                    TongTienThu: chenhLechTraMua,
                    MaHoaDon: maPhieuThuChi,
                    NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                    NguoiNopTien: tenDoiTuong,
                    NguoiTao: hd.NguoiTao,
                    NoiDungThu: ghichu,
                    ID_NhanVien: hd.ID_NhanVien,
                    ID_DonVi: hd.ID_DonVi,
                    // used to get save diary 
                    MaHoaDonTraHang: hd.MaHoaDon,
                    ID_DoiTuong: idDoiTuong,
                }
                myData = {
                    objQuyHoaDon: quyhd,
                    lstCTQuyHoaDon: [qct]
                }

                // phieuchi tralaitien
                let dataReturn = self.shareMoney_QuyHD(ptKhach.PhaiThanhToan, ptKhach.TTBangDiem,
                    formatNumberToFloat(ptKhach.TienMat), formatNumberToFloat(ptKhach.TienPOS),
                    formatNumberToFloat(ptKhach.TienCK), formatNumberToFloat(ptKhach.TienTheGiaTri),
                    0);

                tiendatcoc = dataReturn.TienCoc;
                tienmat = dataReturn.TienMat;
                tienpos = dataReturn.TienPOS;
                tienck = dataReturn.TienChuyenKhoan;
                tienthe = dataReturn.TienTheGiaTri;
                tiendiem = dataReturn.TTBangDiem;
                tongthu = tienmat + tienpos + tienck + tienthe + tiendiem + tiendatcoc;

                if (tienck > 0) {
                    // phieuchi1: ck all
                    let qct2 = newQuyChiTiet({
                        ID_HoaDonLienQuan: null,
                        ID_KhoanThuChi: idKhoanThuChi,
                        ID_DoiTuong: idDoiTuong,
                        GhiChu: ghichu,
                        TienThu: tienck,
                        TienChuyenKhoan: tienck,
                        HinhThucThanhToan: 3,
                        ID_TaiKhoanNganHang: ptKhach.ID_TaiKhoanChuyenKhoan,
                        LoaiThanhToan: chitracoc ? 1 : 0,
                    });
                    lstQuyCT2.push(qct2);
                    phuongthucTT2 += 'Chuyển khoản, ';
                }

                if (tienmat > 0) {
                    let qct2 = newQuyChiTiet({
                        ID_HoaDonLienQuan: null,
                        ID_KhoanThuChi: idKhoanThuChi,
                        ID_DoiTuong: idDoiTuong,
                        GhiChu: ghichu,
                        TienThu: tienmat,
                        TienMat: tienmat,
                        HinhThucThanhToan: 1,
                        LoaiThanhToan: chitracoc ? 1 : 0,
                    });
                    lstQuyCT2.push(qct2)
                    phuongthucTT2 += 'Tiền mặt, ';
                }
                //let nowSeconds = (new Date()).getSeconds() + 1;
                let quyhd2 = {
                    LoaiHoaDon: 11,
                    TongTienThu: tongthu,
                    MaHoaDon: maPhieuThuChi + '_2',
                    NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                    NguoiNopTien: tenDoiTuong,
                    NguoiTao: hd.NguoiTao,
                    NoiDungThu: ghichu,
                    ID_NhanVien: hd.ID_NhanVien,
                    ID_DonVi: hd.ID_DonVi,
                    // used to get save diary 
                    MaHoaDonTraHang: hd.MaHoaDon,
                    ID_DoiTuong: idDoiTuong,
                }
                myData2 = {
                    objQuyHoaDon: quyhd2,
                    lstCTQuyHoaDon: lstQuyCT2
                }
            }
            phuongthucTT = Remove_LastComma(phuongthucTT);
            phuongthucTT2 = Remove_LastComma(phuongthucTT2);

            console.log(1, myData, 2, myData2)
            // phieuchi (neu tra > mua), thu (neu mua > tra)
            ajaxHelper('/api/DanhMuc/Quy_HoaDonAPI/PostQuy_HoaDon_DefaultIDDoiTuong', 'POST', myData).done(function (x) {
                if (x.res === true) {
                    let maPhieuChi = x.data.MaHoaDon;
                    let diary = {
                        LoaiNhatKy: 1,
                        ID_DonVi: myData.objQuyHoaDon.ID_DonVi,
                        ID_NhanVien: myData.objQuyHoaDon.ID_NhanVien,
                        ChucNang: 'Phiếu '.concat(sLoai),
                        NoiDung: 'Tạo phiếu '.concat(sLoai, maPhieuChi, ' cho hóa đơn ', hd.MaHoaDon,
                            ', Khách hàng: ', myData.objQuyHoaDon.NguoiNopTien, ', với giá trị ', formatNumber3Digit(myData.objQuyHoaDon.TongTienThu, 2),
                            ', Phương thức thanh toán:', phuongthucTT,
                            ', Thời gian: ', moment(myData.objQuyHoaDon.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')),
                        NoiDungChiTiet: 'Tạo phiếu ' + sLoai + ' <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD('.concat(maPhieuChi, ')" >', maPhieuChi, '</a> ',
                            ' cho hóa đơn: <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD(', hd.MaHoaDon, ')" >', hd.MaHoaDon, '</a> ',
                            ', Khách hàng: <a style="cursor: pointer" onclick = "LoadKhachHang_byMaKH(', myData.objQuyHoaDon.NguoiNopTien, ')" >', myData.objQuyHoaDon.NguoiNopTien, '</a> ',
                            '<br /> Giá trị: ', formatNumber3Digit(myData.objQuyHoaDon.TongTienThu, 2),
                            '<br/ > Phương thức thanh toán: ', phuongthucTT,
                            '<br/ > Thời gian: ', moment(myData.objQuyHoaDon.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')
                        )
                    }
                    Insert_NhatKyThaoTac_1Param(diary);

                    vmThemMoiKhach.NangNhomKhachHang(idDoiTuong);
                }
            })

            // phieuchi2
            ajaxHelper('/api/DanhMuc/Quy_HoaDonAPI/PostQuy_HoaDon_DefaultIDDoiTuong', 'POST', myData2).done(function (x) {
                if (x.res === true) {
                    let maPhieuChi = x.data.MaHoaDon;
                    let diary = {
                        LoaiNhatKy: 1,
                        ID_DonVi: myData2.objQuyHoaDon.ID_DonVi,
                        ID_NhanVien: myData2.objQuyHoaDon.ID_NhanVien,
                        ChucNang: 'Phiếu chi',
                        NoiDung: 'Tạo phiếu chi '.concat(maPhieuChi, ' cho hóa đơn ', hd.MaHoaDon,
                            ', Khách hàng: ', myData2.objQuyHoaDon.NguoiNopTien, ', với giá trị ', formatNumber3Digit(myData2.objQuyHoaDon.TongTienThu, 2),
                            ', Phương thức thanh toán:', phuongthucTT2, chitracoc ? ' (Trả lại tiền cọc)' : '',
                            ', Thời gian: ', moment(myData2.objQuyHoaDon.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')),
                        NoiDungChiTiet: 'Tạo phiếu chi <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD('.concat(maPhieuChi, ')" >', maPhieuChi, '</a> ',
                            ' cho hóa đơn: <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD(', hd.MaHoaDon, ')" >', hd.MaHoaDon, '</a> ',
                            ', Khách hàng: <a style="cursor: pointer" onclick = "LoadKhachHang_byMaKH(', myData2.objQuyHoaDon.NguoiNopTien, ')" >', myData2.objQuyHoaDon.NguoiNopTien, '</a> ',
                            '<br /> Giá trị: ', formatNumber3Digit(myData2.objQuyHoaDon.TongTienThu, 2),
                            '<br/ > Phương thức thanh toán: ', phuongthucTT2, chitracoc ? ' (Trả lại tiền cọc)' : '',
                            '<br/ > Thời gian: ', moment(myData2.objQuyHoaDon.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')
                        )
                    }
                    Insert_NhatKyThaoTac_1Param(diary);

                    vmThemMoiKhach.NangNhomKhachHang(idDoiTuong);
                }
            })
        },

        SavePhieuThu_Default: function (hd) {
            var loaiThuChi = 11;
            var sLoai = 'thu';
            let lstQuyCT = [];
            let daTT = formatNumberToFloat(hd.DaThanhToan);
            var soduDatCoc = hd.SoDuDatCoc;
            let idDoiTuong = hd.ID_DoiTuong;
            let idHoaDon = hd.ID;
            let tenDoiTuong = hd.TenDoiTuong;
            var ghichu = hd.DienGiai;
            var tiendatcoc = 0;
            var idKhoanThuChi = null;
            if (hd.HoanTraTamUng > 0) {
                loaiThuChi = 12;
                sLoai = 'chi';
            }
            if (daTT > 0 || (soduDatCoc > 0 && loaiThuChi === 11)) {
                let phuongthucTT = '';

                if (soduDatCoc > 0 && loaiThuChi === 11) {
                    let qct = newQuyChiTiet({
                        ID_HoaDonLienQuan: idHoaDon,
                        ID_KhoanThuChi: idKhoanThuChi,
                        ID_DoiTuong: idDoiTuong,
                        GhiChu: ghichu,
                        TienThu: soduDatCoc,
                        TienCoc: soduDatCoc,
                        HinhThucThanhToan: 6,
                    });
                    lstQuyCT.push(qct);
                    phuongthucTT += 'Tiền cọc, ';
                }

                if (daTT > 0) {
                    if (tiendatcoc > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: tiendatcoc,
                            TienCoc: tiendatcoc,
                            HinhThucThanhToan: 1,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT = 'Trả lại tiền đặt cọc';
                    }
                    else {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: daTT,
                            TienMat: daTT,
                            HinhThucThanhToan: 1,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Tiền mặt, ';
                    }
                }

                // tao phieuthu tu HD datcoc
                let nowSeconds = (new Date()).getSeconds() + 1;
                let quyhd = {
                    LoaiHoaDon: loaiThuChi,
                    TongTienThu: daTT + (loaiThuChi == 11 ? soduDatCoc : 0),// chi cong tien coc neu la phieuthu
                    MaHoaDon: 'TT' + hd.MaHoaDon,
                    NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                    NguoiNopTien: tenDoiTuong,
                    NguoiTao: hd.NguoiTao,
                    NoiDungThu: ghichu,
                    ID_NhanVien: hd.ID_NhanVien,
                    ID_DonVi: hd.ID_DonVi,
                    ID_DoiTuong: idDoiTuong,
                }

                var myData = {
                    objQuyHoaDon: quyhd,
                    lstCTQuyHoaDon: lstQuyCT
                };

                ajaxHelper('/api/DanhMuc/Quy_HoaDonAPI/PostQuy_HoaDon_DefaultIDDoiTuong', 'POST', myData).done(function (x) {
                    if (x.res === true) {
                        quyhd.MaHoaDon = x.data.MaHoaDon;
                        let diary = {
                            LoaiNhatKy: 1,
                            ID_DonVi: quyhd.ID_DonVi,
                            ID_NhanVien: quyhd.ID_NhanVien,
                            ChucNang: 'Phiếu ' + sLoai,
                            NoiDung: 'Tạo phiếu '.concat(sLoai, ' ', quyhd.MaHoaDon, ' cho hóa đơn ', hd.MaHoaDon,
                                ', Khách hàng: ', quyhd.NguoiNopTien, ', với giá trị ', formatNumber3Digit(quyhd.TongTienThu, 2),
                                ', Phương thức thanh toán:', phuongthucTT,
                                ', Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')),
                            NoiDungChiTiet: 'Tạo phiếu ' + sLoai + ' <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD('.concat(quyhd.MaHoaDon, ')" >', quyhd.MaHoaDon, '</a> ',
                                ' cho hóa đơn: <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD(', hd.MaHoaDon, ')" >', hd.MaHoaDon, '</a> ',
                                ', Khách hàng: <a style="cursor: pointer" onclick = "LoadKhachHang_byMaKH(', quyhd.NguoiNopTien, ')" >', quyhd.NguoiNopTien, '</a> ',
                                '<br /> Giá trị: ', formatNumber3Digit(quyhd.TongTienThu, 2),
                                '<br/ > Phương thức thanh toán: ', phuongthucTT,
                                '<br/ > Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')
                            )
                        }
                        Insert_NhatKyThaoTac_1Param(diary);
                        vmThemMoiKhach.NangNhomKhachHang(idDoiTuong);
                    }
                })
            }
        },

        SavePhieuThu: async function (nvthHoaDon = []) {
            var self = this;
            var hd = self.inforHoaDon;
            vmThemMoiKhach.inforLogin.ID_DonVi = hd.ID_DonVi;// used to check nangnhomkh

            let ghichu = hd.DienGiai;
            var idHoaDon = hd.ID;
            let idDoiTuong = hd.ID_DoiTuong;
            let tenDoiTuong = hd.TenDoiTuong;
            var ptKhach = self.PhieuThuKhach;
            let idKhoanThuChi = ptKhach.ID_KhoanThuChi;

            var loaiThuChi = 11;
            var sLoai = 'thu';
            var tiendatcoc = formatNumberToFloat(ptKhach.TienDatCoc), soduDatCoc = hd.SoDuDatCoc;
            var maPhieuThuChi = 'TT' + hd.MaHoaDon;
            var chitracoc = self.isCheckTraLaiCoc;
            let tienmat = 0, tienpos = 0, tienck = 0, tienthe = 0, tiendiem = 0, tongthu = 0;

            if (soduDatCoc > 0 && hd.ChenhLechTraMua != 0) {
                self.SavePhieuThu_HDDoiTra();
            }
            else {
                if ((hd.HoanTraTamUng > 0 && soduDatCoc <= 0) || ptKhach.HoanTraTamUng > 0) {
                    loaiThuChi = 12;
                    sLoai = 'chi';
                }
                var khach_PhieuThuTT = hd.PhaiThanhToan;
                if (ptKhach.HoanTraTamUng > 0) {
                    // chitracoc
                    khach_PhieuThuTT = ptKhach.PhaiThanhToan;
                    if (tiendatcoc > 0) {
                        idHoaDon = null; // neu tiencoc > 0 & hoantra tiencho khach --> khong gan idHoaDon (vi no bi bu tru congno)
                    }
                }
                //  used to get save diary
                if (formatNumberToFloat(ptKhach.DaThanhToan) > 0) {
                    let lstQuyCT = [];
                    let phuongthucTT = '';
                    let dataReturn = self.shareMoney_QuyHD(khach_PhieuThuTT, ptKhach.TTBangDiem,
                        formatNumberToFloat(ptKhach.TienMat), formatNumberToFloat(ptKhach.TienPOS),
                        formatNumberToFloat(ptKhach.TienCK), formatNumberToFloat(ptKhach.TienTheGiaTri),
                        ptKhach.HoanTraTamUng > 0 ? 0 : tiendatcoc);// nếu hoàn trả tiền: không gán tiền cọc ở đây

                    tiendatcoc = dataReturn.TienCoc;
                    tienmat = dataReturn.TienMat;
                    tienpos = dataReturn.TienPOS;
                    tienck = dataReturn.TienChuyenKhoan;
                    tienthe = dataReturn.TienTheGiaTri;
                    tiendiem = dataReturn.TTBangDiem;
                    tongthu = tienmat + tienpos + tienck + tienthe + tiendiem + tiendatcoc;

                    //todo: gán lai tiền khách đã trả/ tiền trả khách --> use when print
                    self.PhieuThuKhachPrint.TienDatCoc = tiendatcoc;
                    self.PhieuThuKhachPrint.TienMat = tienmat;
                    self.PhieuThuKhachPrint.TienCK = tienck;
                    self.PhieuThuKhachPrint.TienPOS = tienpos;
                    self.PhieuThuKhachPrint.TienTheGiaTri = tienthe;
                    self.PhieuThuKhachPrint.TTBangDiem = tiendiem;
                    self.PhieuThuKhachPrint.DaThanhToan = tongthu;
                    if (self.LinkQR != '') {
                        self.PhieuThuKhachPrint.TenNganHangCK = ptKhach.TenNganHangCK;
                        self.PhieuThuKhachPrint.TenChuTheCK = ptKhach.TenTaiKhoanCK;
                        self.PhieuThuKhachPrint.SoTaiKhoanCK = ptKhach.SoTaiKhoanCK;
                    }             
                    // thu tiền cọc
                    if (tiendatcoc > 0 && loaiThuChi === 11) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: tiendatcoc,
                            TienCoc: tiendatcoc,
                            HinhThucThanhToan: 6,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT = 'Tiền cọc, ';
                    }

                    if (tienmat > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: tienmat,
                            TienMat: tienmat,
                            HinhThucThanhToan: 1,
                            LoaiThanhToan: chitracoc ? 1 : 0,
                        });
                        phuongthucTT += 'Tiền mặt, ';
                        lstQuyCT.push(qct);
                    }

                    if (tienpos > 0) {
                        khach_idPos = ptKhach.ID_TaiKhoanPos;
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: tienpos,
                            TienPOS: tienpos,
                            HinhThucThanhToan: 2,
                            ID_TaiKhoanNganHang: ptKhach.ID_TaiKhoanPos,
                            LoaiThanhToan: chitracoc ? 1 : 0,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'POS, ';
                    }
                    if (tienck > 0) {
                        khach_idCK = ptKhach.ID_TaiKhoanChuyenKhoan;
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: tienck,
                            TienChuyenKhoan: tienck,
                            HinhThucThanhToan: 3,
                            ID_TaiKhoanNganHang: ptKhach.ID_TaiKhoanChuyenKhoan,
                            LoaiThanhToan: chitracoc ? 1 : 0,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Chuyển khoản, ';
                    }
                    if (tienthe > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: tienthe,
                            TienTheGiaTri: tienthe,
                            HinhThucThanhToan: 4,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Thẻ giá trị, ';
                    }
                    if (tiendiem > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: idHoaDon,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idDoiTuong,
                            GhiChu: ghichu,
                            TienThu: tiendiem,
                            TTBangDiem: tiendiem,
                            HinhThucThanhToan: 5,
                            DiemThanhToan: ptKhach.DiemQuyDoi,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Điểm, ';
                    }

                    //if (commonStatisJs.CheckNull(idDoiTuong) && tongthu < ptKhach.DaThanhToan) {
                    //    commonStatisJs.ShowMessageDanger("Là khách lẻ, không cho phép nợ");
                    //    return;
                    //}
                    let quyhd = {
                        LoaiHoaDon: loaiThuChi,
                        TongTienThu: tongthu,
                        MaHoaDon: maPhieuThuChi,
                        NgayLapHoaDon: hd.NgayLapHoaDon,
                        NguoiNopTien: tenDoiTuong,
                        NguoiTao: hd.NguoiTao,
                        NoiDungThu: ghichu,
                        ID_NhanVien: hd.ID_NhanVien,
                        ID_DonVi: hd.ID_DonVi,
                        // used to get save diary 
                        MaHoaDonTraHang: hd.MaHoaDon,
                        ID_DoiTuong: idDoiTuong,
                    }
                    phuongthucTT = Remove_LastComma(phuongthucTT);
                    quyhd.PhuongThucTT = phuongthucTT;
                    self.PhieuThuKhachPrint.PhuongThucTT = phuongthucTT;

                    let myData = {
                        objQuyHoaDon: quyhd,
                        lstCTQuyHoaDon: lstQuyCT
                    };

                    if (lstQuyCT.length > 0) {
                        const x = await ajaxHelper('/api/DanhMuc/Quy_HoaDonAPI/PostQuy_HoaDon_DefaultIDDoiTuong', 'POST', myData).done()
                            .then(function (xx) {
                                return xx;
                            });
                        if (x.res) {
                            quyhd.MaHoaDon = x.data.MaHoaDon;
                            await self.UpdateIDQuyHoaDon_toBHThucHien(idHoaDon, x.data.ID);

                            let diary = {
                                LoaiNhatKy: 1,
                                ID_DonVi: quyhd.ID_DonVi,
                                ID_NhanVien: quyhd.ID_NhanVien,
                                ChucNang: 'Phiếu ' + sLoai,
                                NoiDung: 'Tạo phiếu '.concat(sLoai, ' ', quyhd.MaHoaDon, ' cho hóa đơn ', hd.MaHoaDon,
                                    ', Khách hàng: ', quyhd.NguoiNopTien, ', với giá trị ', formatNumber3Digit(quyhd.TongTienThu, 2),
                                    ', Phương thức thanh toán:', phuongthucTT, chitracoc ? ' (Trả lại tiền cọc)' : '',
                                    ', Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')),
                                NoiDungChiTiet: 'Tạo phiếu ' + sLoai + ' <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD('.concat(quyhd.MaHoaDon, ')" >', quyhd.MaHoaDon, '</a> ',
                                    ' cho hóa đơn: <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD(', hd.MaHoaDon, ')" >', hd.MaHoaDon, '</a> ',
                                    ', Khách hàng: <a style="cursor: pointer" onclick = "LoadKhachHang_byMaKH(', quyhd.NguoiNopTien, ')" >', quyhd.NguoiNopTien, '</a> ',
                                    '<br /> Giá trị: ', formatNumber3Digit(quyhd.TongTienThu, 2),
                                    '<br/ > Phương thức thanh toán: ', phuongthucTT, chitracoc ? ' (Trả lại tiền cọc)' : '',
                                    '<br/ > Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')
                                )
                            }
                            Insert_NhatKyThaoTac_1Param(diary);
                            vmThemMoiKhach.NangNhomKhachHang(idDoiTuong);
                        }
                    }
                }

                // thu tu coc
                var tienCocX = formatNumberToFloat(ptKhach.TienDatCoc);
                if (ptKhach.HoanTraTamUng > 0 && tienCocX > 0) {
                    let qct = newQuyChiTiet({
                        ID_HoaDonLienQuan: hd.ID,
                        ID_KhoanThuChi: idKhoanThuChi,
                        ID_DoiTuong: idDoiTuong,
                        GhiChu: ghichu,
                        TienThu: tienCocX,
                        TienCoc: tienCocX,
                        HinhThucThanhToan: 6,
                    });
                    lstQuyCT = [qct];
                    let phuongthucTT2 = 'Thu từ cọc';
                    let sLoai2 = 'thu';
                    tongthu = tienCocX;

                    self.PhieuThuKhachPrint.TienDatCoc = tienCocX;
                    self.PhieuThuKhachPrint.DaThanhToan = tienCocX;

                    let quyhd = {
                        LoaiHoaDon: 11,
                        TongTienThu: tongthu,
                        MaHoaDon: maPhieuThuChi,
                        NgayLapHoaDon: hd.NgayLapHoaDon,
                        NguoiNopTien: tenDoiTuong,
                        NguoiTao: hd.NguoiTao,
                        NoiDungThu: ghichu,
                        ID_NhanVien: hd.ID_NhanVien,
                        ID_DonVi: hd.ID_DonVi,
                        // used to get save diary 
                        MaHoaDonTraHang: hd.MaHoaDon,
                        ID_DoiTuong: idDoiTuong,
                    }
                    quyhd.PhuongThucTT = phuongthucTT2;

                    let myData = {
                        objQuyHoaDon: quyhd,
                        lstCTQuyHoaDon: lstQuyCT
                    };

                    if (lstQuyCT.length > 0) {
                        console.log('myData_quyKH ', myData);
                        const xCoc = await ajaxHelper('/api/DanhMuc/Quy_HoaDonAPI/PostQuy_HoaDon_DefaultIDDoiTuong', 'POST', myData).done()
                            .then(function (x) {
                                return x;
                            });
                        if (xCoc.res) {
                            await self.UpdateIDQuyHoaDon_toBHThucHien(lstQuyCT[0].ID_HoaDonLienQuan, xCoc.data.ID);
                            quyhd.MaHoaDon = xCoc.data.MaHoaDon;
                            let diary = {
                                LoaiNhatKy: 1,
                                ID_DonVi: quyhd.ID_DonVi,
                                ID_NhanVien: quyhd.ID_NhanVien,
                                ChucNang: 'Phiếu ' + sLoai2,
                                NoiDung: 'Tạo phiếu '.concat(sLoai2, ' ', quyhd.MaHoaDon, ' cho hóa đơn ', hd.MaHoaDon,
                                    ', Khách hàng: ', quyhd.NguoiNopTien, ', với giá trị ', formatNumber3Digit(quyhd.TongTienThu, 2),
                                    ', Phương thức thanh toán:', phuongthucTT2,
                                    ', Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')),
                                NoiDungChiTiet: 'Tạo phiếu ' + sLoai2 + ' <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD('.concat(quyhd.MaHoaDon, ')" >', quyhd.MaHoaDon, '</a> ',
                                    ' cho hóa đơn: <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD(', hd.MaHoaDon, ')" >', hd.MaHoaDon, '</a> ',
                                    ', Khách hàng: <a style="cursor: pointer" onclick = "LoadKhachHang_byMaKH(', quyhd.NguoiNopTien, ')" >', quyhd.NguoiNopTien, '</a> ',
                                    '<br /> Giá trị: ', formatNumber3Digit(quyhd.TongTienThu, 2),
                                    '<br/ > Phương thức thanh toán: ', phuongthucTT2,
                                    '<br/ > Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')
                                )
                            }
                            Insert_NhatKyThaoTac_1Param(diary);
                            vmThemMoiKhach.NangNhomKhachHang(idDoiTuong);
                        }
                    }
                }

                let ptBaoHiem = self.PhieuThuBaoHiem;
                if (ptBaoHiem.DaThanhToan > 0) {
                    let lstQuyCT = [];
                    let phuongthucTT = '';
                    let idBaoHiem = hd.ID_BaoHiem;
                    idBaoHiem = idBaoHiem === null ? const_GuidEmpty : idBaoHiem;

                    let dataReturn = self.shareMoney_QuyHD(hd.PhaiThanhToanBaoHiem, formatNumberToFloat(ptBaoHiem.TTBangDiem),
                        formatNumberToFloat(ptBaoHiem.TienMat), formatNumberToFloat(ptBaoHiem.TienPOS),
                        formatNumberToFloat(ptBaoHiem.TienCK), formatNumberToFloat(ptBaoHiem.TienTheGiaTri),
                        formatNumberToFloat(ptBaoHiem.TienDatCoc));
                    tiendatcoc = dataReturn.TienCoc;
                    tienmat = dataReturn.TienMat;
                    tienpos = dataReturn.TienPOS;
                    tienck = dataReturn.TienChuyenKhoan;
                    tienthe = dataReturn.TienTheGiaTri;
                    tiendiem = dataReturn.TTBangDiem;
                    tongthu = tienmat + tienpos + tienck + tienthe + tiendiem;

                    self.PhieuThuBaoHiemPrint.TienDatCoc = tiendatcoc;
                    self.PhieuThuBaoHiemPrint.TienMat = tienmat;
                    self.PhieuThuBaoHiemPrint.TienCK = tienck;
                    self.PhieuThuBaoHiemPrint.TienPOS = tienpos;
                    self.PhieuThuBaoHiemPrint.TienTheGiaTri = tienthe;
                    self.PhieuThuBaoHiemPrint.TTBangDiem = tiendiem;
                    self.PhieuThuBaoHiemPrint.DaThanhToan = tongthu;

                    if (tienmat > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: hd.ID,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idBaoHiem,
                            GhiChu: ghichu,
                            TienThu: tienmat,
                            TienMat: tienmat,
                            HinhThucThanhToan: 1,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Tiền mặt, ';
                    }
                    if (tienpos > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: hd.ID,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idBaoHiem,
                            GhiChu: ghichu,
                            TienThu: tienpos,
                            TienPOS: tienpos,
                            HinhThucThanhToan: 2,
                            ID_TaiKhoanNganHang: ptBaoHiem.ID_TaiKhoanPos,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'POS, ';
                    }
                    if (tienck > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: hd.ID,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idBaoHiem,
                            GhiChu: ghichu,
                            TienThu: tienck,
                            TienChuyenKhoan: tienck,
                            HinhThucThanhToan: 3,
                            ID_TaiKhoanNganHang: ptBaoHiem.ID_TaiKhoanChuyenKhoan,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Chuyển khoản, ';
                    }
                    if (tienthe > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: hd.ID,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idBaoHiem,
                            GhiChu: ghichu,
                            TienThu: tienthe,
                            TienTheGiaTri: tienthe,
                            HinhThucThanhToan: 4,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Thẻ giá trị, ';
                    }
                    if (tiendiem > 0) {
                        let qct = newQuyChiTiet({
                            ID_HoaDonLienQuan: hd.ID,
                            ID_KhoanThuChi: idKhoanThuChi,
                            ID_DoiTuong: idBaoHiem,
                            GhiChu: ghichu,
                            TienThu: tiendiem,
                            TTBangDiem: tiendiem,
                            HinhThucThanhToan: 5,
                            DiemThanhToan: ptBaoHiem.DiemQuyDoi,
                        });
                        lstQuyCT.push(qct);
                        phuongthucTT += 'Điểm, ';
                    }

                    if (ptKhach.DaThanhToan > 0) {
                        maPhieuThuChi = 'TT' + hd.MaHoaDon + '_2';
                    }

                    let nowSeconds = (new Date()).getSeconds() + 1;
                    let quyhd = {
                        LoaiHoaDon: 11,
                        TongTienThu: tongthu,
                        MaHoaDon: maPhieuThuChi,
                        NgayLapHoaDon: moment(hd.NgayLapHoaDon).add(nowSeconds, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
                        NguoiNopTien: hd.TenBaoHiem,
                        NguoiTao: hd.NguoiTao,
                        NoiDungThu: ghichu,
                        ID_NhanVien: hd.ID_NhanVien,
                        ID_DonVi: hd.ID_DonVi,
                        ID_DoiTuong: idBaoHiem,
                    };
                    phuongthucTT = Remove_LastComma(phuongthucTT);
                    quyhd.PhuongThucTT = phuongthucTT;
                    self.PhieuThuBaoHiemPrint.PhuongThucTT = phuongthucTT;

                    var myData = {
                        objQuyHoaDon: quyhd,
                        lstCTQuyHoaDon: lstQuyCT
                    };
                    if (lstQuyCT.length > 0) {
                        console.log('myData_quyBH ', myData);
                        const xThuBaoHiem = await ajaxHelper('/api/DanhMuc/Quy_HoaDonAPI/PostQuy_HoaDon_DefaultIDDoiTuong', 'POST', myData).done()
                            .then(function (x) {
                                return x;
                            });
                        if (xThuBaoHiem.res) {
                            quyhd.MaHoaDon = xThuBaoHiem.data.MaHoaDon;
                            let diary = {
                                LoaiNhatKy: 1,
                                ID_DonVi: quyhd.ID_DonVi,
                                ID_NhanVien: quyhd.ID_NhanVien,
                                ChucNang: 'Phiếu thu',
                                NoiDung: 'Tạo phiếu thu '.concat(quyhd.MaHoaDon, ' cho hóa đơn ', hd.MaHoaDon,
                                    ', Cty bảo hiểm: ', quyhd.NguoiNopTien, ', với giá trị ', formatNumber3Digit(quyhd.TongTienThu, 2),
                                    ', Phương thức thanh toán:', phuongthucTT,
                                    ', Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')),
                                NoiDungChiTiet: 'Tạo phiếu thu <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD('.concat(quyhd.MaHoaDon, ')" >', quyhd.MaHoaDon, '</a> ',
                                    ' cho hóa đơn: <a style="cursor: pointer" onclick = "LoadHoaDon_byMaHD(', hd.MaHoaDon, ')" >', hd.MaHoaDon, '</a> ',
                                    ', Cty bảo hiểm: <a style="cursor: pointer" onclick = "LoadKhachHang_byMaKH(', quyhd.NguoiNopTien, ')" >', quyhd.NguoiNopTien, '</a> ',
                                    '<br /> Giá trị:', formatNumber3Digit(quyhd.TongTienThu, 2),
                                    '<br/ > Phương thức thanh toán:', phuongthucTT,
                                    '<br/ > Thời gian: ', moment(quyhd.NgayLapHoaDon).format('DD/MM/YYYY HH:mm')
                                )
                            }
                            Insert_NhatKyThaoTac_1Param(diary);
                        }
                    }
                }

                if ((ptKhach.DaThanhToan === undefined || ptKhach.DaThanhToan === 0)
                    && (ptBaoHiem.DaThanhToan === undefined || ptBaoHiem.DaThanhToan === 0) && hd.ID_PhieuTiepNhan === null) {
                    //self.SavePhieuThu_Default(hd);
                }

                if (nvthHoaDon.length > 0 && idHoaDon !== null &&
                    (commonStatisJs.CheckNull(ptKhach.DaThanhToan) || ptKhach.DaThanhToan === 0)
                    && (commonStatisJs.CheckNull(ptBaoHiem.DaThanhToan) || ptBaoHiem.DaThanhToan === 0)
                ) {
                    // only get cktheo doanhthu
                    self.GridNVienBanGoi_Chosed = nvthHoaDon.filter(x => x.TinhChietKhauTheo !== 1);
                    await self.UpdateIDQuyHoaDon_toBHThucHien(idHoaDon);
                }
            }

            $('#ThongTinThanhToanKHNCC').modal('hide');
        },
        //GetQRCodeLink: function (type = 1) {
        //    let self = this;
        //    if (type === 1) {
        //        //QR khách hàng
        //        self.QRCode.SoTien = self.PhieuThuKhach.TienCK;
        //    }
        //    else if (type === 2) {
        //        //QR bảo hiểm
        //        self.QRCodeBH.SoTien = self.PhieuThuBaoHiem.TienCK;
        //    }

        //},
        ShowModalQRCode: function (type = 1) {
            let self = this;
            if (type === 1) {
                //QR khách hàng
                self.QRCode.SoTien = self.PhieuThuKhach.TienCK;
                VQRCode.setData(self.QRCode);
            }
            else if (type === 2) {
                //QR bảo hiểm
                self.QRCodeBH.SoTien = self.PhieuThuBaoHiem.TienCK;
                VQRCode.setData(self.QRCodeBH);
            }
            VQRCode.showModal();
        },

        updateQRCode: async function () {
            let self = this;
            self.LinkQR = await getQRCode({
                accountNo: self.QRCode.SoTaiKhoan,
                accountName: self.QRCode.TenNganHangCK,
                acqId: self.QRCode.MaPin,
                addInfo: self.QRCode.NoiDung,
                amount: self.QRCode.SoTien
            });                
        },

    },
})
