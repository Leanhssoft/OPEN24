var vmThemMoiNguoiDung = new Vue({
    el: '#vmThemMoiNguoiDung',
    components: {
        'nhanviens': ComponentChoseStaff,
        'chinhanhs': cmpChiNhanh,
        'vaitros': cmpVaiTro,
    },
    data: {
        saveOK: false,
        typeUpdate: true,// 1,new, 2.update, 3.delete
        isSaving: false,
        isChangePassword: false,
        role: {},
        itemOld: {},
        chinhanhChosing: { TenChiNhanh: '', ID: null },

        listData: {
            NhanViens: [],
            ChiNhanhs: [],
            NhomNguoiDungs: [],
        },
        newUser: {
            ID: null,
            ID_NhanVien: null,
            ID_DonVi: null,
            ID_NhomNguoiDung: null,
            TaiKhoan: '',
            MatKhau: '',
            LaNhanVien: true,
            LaAdmin: false,
            DangHoatDong: true,
            NguoiTao: VHeader.UserLogin,
            NgayTao: new Date(),
            NguoiSua: '',
            XemGiaVon: false,
            IsSystem: false,

            TenNhanVien: '',
            TenVaiTro: '',
            TenDonVi: '',
        },
    },
    created: function () {
        var self = this;
        self.HTNguoiDungAPI = '/api/DanhMuc/HT_NguoiDungAPI/';
        self.Guid_Empty = '00000000-0000-0000-0000-000000000000';
    },
    methods: {
        GetListDonVi_byNhanVien: async function (idNhanVien) {
            var self = this;
            await ajaxHelper("/api/DanhMuc/DM_DonViAPI/" + "GetListDonViByIDNguoiDung?idnhanvien=" + idNhanVien, 'GET').done()
                .then(function (data) {
                    if (data !== null && data.length > 0) {
                        self.listData.ChiNhanhs = data;
                    }
                    else {
                        self.listData.ChiNhanhs = [];
                    }
                })
        },
        ShowModalAdd: function () {
            var self = this;
            self.saveOK = false;
            self.typeUpdate = 1;
            self.isSaving = false;
            self.isChangePassword = false;

            self.newUser = {
                ID: self.Guid_Empty,
                ID_NhanVien: null,
                ID_DonVi: null,
                ID_NhomNguoiDung: null,
                TaiKhoan: '',
                MatKhau: '',
                PasswordRepeat: '',
                LaNhanVien: true,
                LaAdmin: false,
                DangHoatDong: true,
                NguoiTao: VHeader.UserLogin,
                NgayTao: new Date(),
                NguoiSua: '',
                XemGiaVon: false,
                IsSystem: false,

                MaNhanVien: '',
                TenNhanVien: '',
                TenVaiTro: '',
                TenDonVi: '',
            }
            $('#vmThemMoiNguoiDung').modal('show');
        },
        ShowModalUpdate: async function (item) {
            var self = this;
            self.saveOK = false;
            self.typeUpdate = 2;
            self.isChangePassword = false;
            self.itemOld = $.extend({}, item);
            self.newUser = {
                ID: item.ID,
                ID_NhanVien: item.ID_NhanVien,
                ID_DonVi: item.ID_DonVi,
                TaiKhoan: item.TaiKhoan,
                MatKhau: item.MatKhau,
                PasswordRepeat: item.MatKhau,
                LaNhanVien: true,
                LaAdmin: false,
                IsSystem: false,
                DangHoatDong: item.DangHoatDong,
                NguoiSua: VHeader.UserLogin,
                XemGiaVon: false,
                ID_NhomNguoiDung: item.ID_NhomNguoiDung,

                MaNhanVien: item.MaNhanVien,
                TenNhanVien: item.TenNhanVien,
                TenDonVi: VHeader.TenDonVi,
                TenVaiTro: '',
            }
            var vaitro = $.grep(self.listData.NhomNguoiDungs, function (x) {
                return x.ID === self.newUser.ID_NhomNguoiDung;
            });
            if (vaitro.length > 0) {
                self.newUser.TenVaiTro = vaitro[0].TenNhom;
            }
            await self.GetListDonVi_byNhanVien(item.ID_NhanVien);
            let itemDV = self.listData.ChiNhanhs.filter((x) => x.ID === item.ID_DonVi);
            if (itemDV.length > 0) {
                self.newUser.TenDonVi = itemDV[0].TenDonVi;
                self.itemOld.TenDonVi = itemDV[0].TenDonVi;
            }
            $('#vmThemMoiNguoiDung').modal('show');
        },

        ChoseNhanVien: async function (item) {
            let self = this;
            self.newUser.MaNhanVien = item.MaNhanVien;
            self.newUser.TenNhanVien = item.TenNhanVien;
            self.newUser.ID_NhanVien = item.ID;
            await self.GetListDonVi_byNhanVien(item.ID);
            if (self.listData.ChiNhanhs.length === 1) {
                self.newUser.TenDonVi = self.listData.ChiNhanhs[0].TenDonVi;
                self.newUser.ID_DonVi = self.listData.ChiNhanhs[0].ID;
            }
        },
        ChoseVaiTro: function (item) {
            let self = this;
            self.newUser.TenVaiTro = item.TenNhom;
            self.newUser.ID_NhomNguoiDung = item.ID;
        },
        ChoseChiNhanh: function (item) {
            let self = this;
            self.newUser.TenDonVi = item.TenDonVi;
            self.newUser.ID_DonVi = item.ID;
        },
        CheckSave: function () {
            let self = this;
            var password = self.newUser.MatKhau;
            if (commonStatisJs.CheckNull(self.newUser.ID_NhanVien)) {
                commonStatisJs.ShowMessageDanger('Vui lòng chọn nhân viên');
                return false;
            }
            if (commonStatisJs.CheckNull(self.newUser.ID_DonVi)) {
                commonStatisJs.ShowMessageDanger('Vui lòng chọn chi nhánh');
                return false;
            }
            if (commonStatisJs.CheckNull(self.newUser.TaiKhoan)) {
                commonStatisJs.ShowMessageDanger('Vui lòng nhập tên tài khoản');
                return false;
            }

            var specialChars = "<>!#$%^&*()_+[]{}?:;|'\"\\,./~`-=' 'àáạảãâầấậẩẫăằặẳẵèẽẹẻêếềểễệíịìĩỉòóỏõọôốồộổỗùúụủũỳýỷỹỵđÀÁẠẢÃÂẦẤẬẨẪĂẰẶẲẴÈẼẸẺÊẾỀỂỄỆÍỊÌĨỈÒÓỎÕỌÔỐỒỘỔỖÙÚỤỦŨỲÝỶỸỴĐ"
            for (i = 0; i < specialChars.length; i++) {
                if (self.newUser.TaiKhoan.indexOf(specialChars[i]) > -1) {
                    commonStatisJs.ShowMessageDanger('Tên đăng nhập không được chứa ký tự đặc biệt');
                    return false;
                }
            }

            if (self.isChangePassword || self.typeUpdate == 1) {
                password = password.trim();
                if (password.length < 6) {
                    commonStatisJs.ShowMessageDanger('Mật khẩu phải chứa ít nhất 6 ký tự ');
                    return false;
                }
                if (password.length > 50) {
                    commonStatisJs.ShowMessageDanger('Mật khẩu phải chỉ được nhập tối đa 50 ký tự ');
                    return;
                }
                if (password.search(/\d/) == -1) {
                    commonStatisJs.ShowMessageDanger('Mật khẩu phải có ít nhất 1 chữ số ');
                    return false;
                }

                if (password.search(/[a-zA-Z]/) == -1) {
                    commonStatisJs.ShowMessageDanger('Mật khẩu phải có ít nhất 1 chữ cái');
                    return false;
                }

                if (self.newUser.MatKhau !== self.newUser.PasswordRepeat) {
                    commonStatisJs.ShowMessageDanger('Mật khẩu xác nhận không giống nhau');
                    return false;
                }

                if (commonStatisJs.CheckNull(password)) {
                    commonStatisJs.ShowMessageDanger('Vui lòng nhập mật khẩu');
                    return false;
                }
            }
            else {
                self.newUser.MatKhau = '';
            }

            if (self.typeUpdate === 1) {
                if (commonStatisJs.CheckNull(self.newUser.ID_NhomNguoiDung)) {
                    commonStatisJs.ShowMessageDanger('Vui lòng chọn vai trò');
                    return false;
                }
            }

            return true;
        },
        SaveUser: function () {
            var self = this;
            var check = self.CheckSave();
            if (!check) {
                return;
            }
            self.isSaving = true;
            if (self.typeUpdate === 1) {
                ajaxHelper(self.HTNguoiDungAPI + 'GioiHanSoNguoiDung', 'GET').done(function (data) {
                    if (data) {
                        commonStatisJs.ShowMessageDanger('Cửa hàng đã đạt số người dùng quy định, không thể thêm mới');
                        self.isSaving = false;
                        return;
                    }

                    var myData = {
                        objNewND: self.newUser,
                        id: self.newUser.ID_NhomNguoiDung,
                        idnhanvien: self.newUser.ID_NhanVien,
                    };
                    ajaxHelper(self.HTNguoiDungAPI + "Check_ID_NhanVienExist?idNhanVien=" + self.newUser.ID_NhanVien, 'GET').done(function (data) {
                        if (data) {
                            commonStatisJs.ShowMessageDanger('Mỗi nhân viên chỉ được tạo 1 tài khoản');
                            self.isSaving = false;
                            return false;
                        }
                        ajaxHelper(self.HTNguoiDungAPI + "Check_TenTaiKhoanAddExist?tenTaiKhoan=" + self.newUser.TaiKhoan, 'GET').done(function (data) {
                            if (data) {
                                commonStatisJs.ShowMessageDanger('Tài khoản đã tồn tại');
                                self.isSaving = false;
                                return false;
                            }
                            ajaxHelper(self.HTNguoiDungAPI + "PostHT_NguoiDung", 'POST', myData).done(function (x) {
                                if (x.res) {
                                    self.newUser.ID = x.data.ID;

                                    var diary = {
                                        ID_DonVi: VHeader.IdDonVi,
                                        ID_NhanVien: VHeader.IdNhanVien,
                                        LoaiNhatKy: 1,
                                        ChucNang: 'Quản lý người dùng',
                                        NoiDung: 'Thêm mới người dùng '.concat(self.newUser.TaiKhoan),
                                        NoiDungChiTiet: 'Thêm mới người dùng '.concat(self.newUser.TaiKhoan,
                                            '<br /> - Nhân viên: ', self.newUser.TenNhanVien,
                                            '<br /> - Vai trò: ', self.newUser.TenVaiTro,
                                            '<br /> - Chi nhánh: ', self.newUser.TenDonVi,
                                        )
                                    }
                                    Insert_NhatKyThaoTac_1Param(diary);
                                }
                                self.saveOK = true;
                                commonStatisJs.ShowMessageSuccess('Thêm người dùng thành công');
                            }).fail(function (x) {
                                commonStatisJs.ShowMessageDanger('Thêm người dùng thất bại');
                            }).always(function (x) {
                                self.isSaving = false;
                                $('#vmThemMoiNguoiDung').modal('hide');
                            })
                        });
                    }).always(function () {
                        self.isSaving = false;
                    });
                });
            }
            else {
                var myData = {
                    id: self.newUser.ID,
                    idnhanvien: self.newUser.ID_NhanVien,
                    objNewND: self.newUser,
                };
                ajaxHelper(self.HTNguoiDungAPI + "Check_ID_NhanVienEditExist?idNhanVien=" + self.newUser.ID_NhanVien
                    + "&id=" + self.newUser.ID, 'GET').done(function (data) {
                        if (data) {
                            commonStatisJs.ShowMessageDanger('Tài khoản đã tồn tại');
                            self.isSaving = false;
                            return;
                        }

                        ajaxHelper(self.HTNguoiDungAPI + "Check_TenTaiKhoanExist?tenTaiKhoan=" + self.newUser.TaiKhoan
                            + "&id=" + self.newUser.ID, 'GET').done(function (data) {
                                if (data) {
                                    commonStatisJs.ShowMessageDanger('Tài khoản đã tồn tại');
                                    self.isSaving = false;
                                    return;
                                }
                                ajaxHelper(self.HTNguoiDungAPI + "PutHT_NguoiDung", 'POST', myData).done(function (data) {
                                    self.isSaving = false;
                                    self.saveOK = true;
                                    commonStatisJs.ShowMessageSuccess('Cập nhật người dùng thành công');

                                    let diary = {
                                        ID_DonVi: VHeader.IdDonVi,
                                        ID_NhanVien: VHeader.IdNhanVien,
                                        LoaiNhatKy: 2,
                                        ChucNang: 'Quản lý người dùng',
                                        NoiDung: 'Cập nhật người dùng '.concat(self.newUser.TaiKhoan),
                                        NoiDungChiTiet: 'Cập nhật người dùng '.concat(self.newUser.TaiKhoan,
                                            '<br /> - Mã nhân viên: ', self.newUser.MaNhanVien,
                                            '<br /> - Tên nhân viên: ', self.newUser.TenNhanVien,
                                            '<br /> - Vai trò: ', self.newUser.TenVaiTro,
                                            '<br /> - Chi nhánh: ', self.newUser.TenDonVi,
                                            '<br /> - Đổi mật khẩu: ', self.isChangePassword ? 'có' : 'không',
                                            '<br /> <b> Thông tin cũ: </b> ',
                                            '<br /> - Tên đăng nhập: ', self.itemOld.TaiKhoan,
                                            '<br /> - Mã nhân viên: ', self.itemOld.MaNhanVien,
                                            '<br /> - Tên nhân viên: ', self.itemOld.TenNhanVien,
                                            '<br /> - Chi nhánh: ', self.itemOld.TenDonVi
                                        )
                                    }
                                    Insert_NhatKyThaoTac_1Param(diary);
                                }).always(function () {
                                    $('#vmThemMoiNguoiDung').modal('hide');
                                })
                            })
                    });
            }

        },
    }
})
