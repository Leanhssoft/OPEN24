var componentListGroup = {
    props: {
        ID: { default: null },
        TenNhomHangHoa: { default: '' },
        isChose: { default: '' },
    },
    template: `
        <li v-on:click="choseGroup">
             <a href="javascript:void(0)" class="gara-component-item-nv">
                 <div class="flex flex-between">
                 <span class="seach-hh" style="color:black">{{TenNhomHangHoa}}</span>
                     <span v-if="isChose" class="span-check"> <i class="fa fa-check">  </i> </span>
                </div>                
            </a>
        </li>
`,
    methods: {
        choseGroup: function () {
            this.$emit('chose-group', this);
        }
    }
}
var ComponentChoseGroupofProducts = {
    props: {
        groupName: { default: '' },
        textSearch: { default: '' },
        groups: { default: [] },
        searchList: { default: [] },
        idChosing: { default: null },
        showbuttonReset: { default: false },
    },
    components: {
        'groups': componentListGroup,
    },
    template: `
    <div class="gara-bill-infor-button shortlabel">
         <div class="gara-absolute-button" style="text-align: center" v-on:click="resetItemChose" v-if="showbuttonReset">
                <a class="gara-button-icon">
                 <i class="fa fa-times" style="color:red"></i>
                </a>
            </div> 
        <input class="gara-search-HH " placeholder="Chọn nhóm hàng hóa" style="padding-right: 27px!important"
                onclick= "this.select()"
                v-model="textSearch" v-on:keyup="searchGroup" v-on:click="showList" />
        <div class="gara-search-dropbox drop-search ">
               <ul>
                <groups v-for="(item, index) in searchList"
                           v-bind:id="item.ID"                       
                           v-bind:ten-nhom-hang-hoa="item.TenNhomHangHoa"
                           v-bind:is-chose="idChosing == item.ID"
                           v-on:chose-group="ChangeGroup(item)"></groups>
                 </ul>
        </div>
    </div>
`,
    data: function () {
        return {
            idChosing: null,
            textSearch: '',
            groups: [],
            searchList: [],
        }
    },
    methods: {
        showList: function () {
            $(event.currentTarget).next().show();
        },
        searchGroup: function () {
            var self = this;
            if (commonStatisJs.CheckNull(self.textSearch)) {
                self.searchList = self.groups.slice(0, 20);
                self.idChosing = null;
                self.$emit('reset-item-chose');
            }
            else {
                let txt = commonStatisJs.convertVieToEng(self.textSearch);
                self.searchList = self.groups.filter(e =>
                    commonStatisJs.convertVieToEng(e.TenNhomHangHoa).indexOf(txt) >= 0
                    || e.TenNhomHangHoa.indexOf(txt) >= 0
                );
            }
        },
        ChangeGroup: function (item) {
            this.idChosing = item.ID;
            this.$emit('change-group-parent', item);
            $(event.currentTarget).closest('div').hide();
        },
        resetItemChose: function () {
            let self = this;
            self.groups = [];
            self.idChosing = null;
            self.$emit('reset-item-chose');
        }
    }
};



var vmThemMoiNhomHHNguoiDung = new Vue({
    el: '#ThemNhomHHNguoidung',
    components: {
        'nhomhhs': ComponentChoseGroupofProducts,
    },
    created: function () {

    },
    data: {
        saveOK: false,
        isNew: true,
        typeUpdate: 0,//1.themmoi, 2.update, 0.delete
        isLoading: false,

        NguoiDungChosed: [],
        groupOld: {},

        listData: {
            NhomHangHoas: [],
            NhomNguoiDungs: [],
        },
        newGroupProduct: {
            ID: null,
            TenNhomHangHoa: null,
            ID_NhomHangHoa: null,
            ID_NhanVien: null
        },
    },
    methods: {
        showModalAdd: function () {
            var self = this;
            self.listData.NhomHangHoas = JSON.parse(localStorage.getItem('lc_NhomHangHoas')) || [];
            self.isNew = true;
            self.typeUpdate = 1;
            self.saveOK = false;
            self.isLoading = false;
            self.error = '';
            self.groupOld = {};
            self.NguoiDungChosed = [];
            self.newGroupProduct = {
                ID: '00000000-0000-0000-0000-000000000000',
                TenNhomHangHoa: '',
                ID_NhomHangHoa: '',
                ID_NhanVien: ''
            };
            $('#ThemNhomHHNguoidung').modal('show');
        },
        showModalUpdate: async function (nhomHang) {
            const self = this;
            self.isNew = false;
            self.newGroupProduct = nhomHang;
            self.groupOld = $.extend({}, nhomHang);
            if (self.groupOld.ID_NhanVien.length !== self.listData.NhomNguoiDungs.length) {
                self.NguoiDungChosed = self.groupOld.ID_NhanVien;
            }
            else {
                self.NguoiDungChosed = [];
            }
            self.typeUpdate = 2;
            // Hiển thị modal
            $('#ThemNhomHHNguoidung').modal('show');
        },

        ChoseNhomHH: async function (item) {
            let self = this
            self.newGroupProduct.TenNhomHangHoa = item.TenNhomHangHoa;
            self.newGroupProduct.ID_NhomHangHoa = item.ID;
        },
        ChoseNguoiDung: function (item) {
            var self = this;
            if (item === null) {
                self.NguoiDungChosed = [];
            }
            else {
                if ($.inArray(item.ID, self.ListIDNguoiDung) === -1) {
                    self.NguoiDungChosed.push(item);
                }
            }
        },
        RemoveUser: function (item, index) {
            var self = this;
            self.NguoiDungChosed.splice(index, 1);
        },
        DeleteNhomHHNguoiDung: function () {
            var self = this;
            dialogConfirm('Thông báo xóa', 'Bạn có chắc chắn muốn xóa nhóm hàng hóa người dùng <b>'
                + self.newGroupProduct.TenNhomHangHoa + ' </b> không?', function () {
                    self.typeUpdate = 0;
                    self.saveOK = true;
                    ajaxHelper('/api/DanhMuc/DM_NhomHangHoaAPI/DeleteNhomHangHoaById?idNhomHang=' + self.newGroupProduct.ID_NhomHangHoa, 'DELETE'
                    ).done(function (msg) {
                        $('#ThemNhomHHNguoidung').modal('hide');
                        if (msg === "") {
                            ShowMessage_Success('Xóa nhóm hàng hóa người dùng thành công ');
                            var diary = {
                                ID_DonVi: $('#txtDonVi').val(),
                                ID_NhanVien: $('#txtIDNhanVien').val(),
                                LoaiNhatKy: 3,
                                ChucNang: 'Nhóm hàng hóa người dùng',
                                NoiDung: 'Xóa nhóm hàng hóa người dùng '.concat(self.newGroupProduct.TenNhomHangHoa),
                                NoiDungChiTiet: 'Xóa nhóm hàng hóa người dùng '.concat(self.newGroupProduct.TenNhomHangHoa, ', Người xóa: ', self.inforLogin.UserLogin),
                            };
                            Insert_NhatKyThaoTac_1Param(diary);
                        }
                        else {
                            ShowMessage_Danger(msg);
                        }
                    });
                })
        },
        SaveNhomHHNguoiDung: function () {
            var self = this;
            var tenNhom = self.newGroupProduct.TenNhomHangHoa;

            if (tenNhom === '' || tenNhom === undefined) {
                ShowMessage_Danger('Vui lòng chọn nhóm hàng hóa');
                return;
            }

            var tb = "Thêm mới";
            var idNhomDT = self.newGroupProduct.ID;
            if (idNhomDT === undefined || idNhomDT === const_GuidEmpty) {
                idNhomDT = null;
            }
            if (idNhomDT !== null) {
                tb = "Cập nhật";
            }

            // save NguoiDung
            var arrID = [];
            if (self.NguoiDungChosed.length === 0) {
                // get all NguoiDung
                for (let i = 0; i < self.listData.NhomNguoiDungs.length; i++) {
                    arrID.push(self.listData.NhomNguoiDungs[i].ID);
                }
            }
            else {
                for (let i = 0; i < self.NguoiDungChosed.length; i++) {
                    arrID.push(self.NguoiDungChosed[i].ID);
                }
            }

            var myData = {};
            myData.ID_NhomHangHoa = self.newGroupProduct.ID_NhomHangHoa;
            myData.lstIDNguoiDung = arrID;

            ajaxHelper('/api/DanhMuc/DM_NhomHangHoaAPI/PostHTQuyenNguoiDungNhomHangHoa', 'POST', myData)
                .done(function (data) {
                    self.saveOK = true;
                    self.newGroupProduct.ID_NhanVien = self.NguoiDungChosed;
                    ShowMessage_Success(tb + ' nhóm hàng hóa người dùng thành công');
                })
                .fail(function (error) {
                    self.saveOK = true;
                    ShowMessage_Danger(tb + ' nhóm hàng hóa người dùng không thành công')
                });
            $("#ThemNhomHHNguoidung").modal("hide");
        }

    },
    computed: {
        ListIDNguoiDung: function () {
            return vmThemMoiNhomHHNguoiDung.NguoiDungChosed.map(function (x) { return x.ID });
        }
    }
})


