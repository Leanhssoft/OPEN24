var vmThongBao = new Vue({
    created: function () {
        let self = this;
        console.log('vmThongBao')

        self.hasHeader = true;
        self.SubDomain = '';
        self.tblSetUpThongBao = [];

        if (commonStatisJs.CheckNull($('#subDomain').val())) {
            self.SubDomain = VHeader.SubDomain.toLowerCase();
        }
        else {
            self.hasHeader = false;
            self.SubDomain = $('#subDomain').val().toLowerCase();
        }

        if (self.SubDomain === '0973474985') {
            $.getJSON('/api/DanhMuc/HT_ThietLapAPI/LeeAuto_GetCaiDatNhacTienDo').done(function (obj) {
                if (obj.res) {
                    obj.dataSoure.map(function (x) {
                        x['STT'] = x.LoaiThongBao
                        x['ID_QuyTrinhTruoc'] = x.LoaiThoiGianLapLai
                        x['ID_QuyTrinhSau'] = x.SoLanLapLai
                        x['ThoiGian'] = x.NhacTruocThoiGian
                        x['LoaiThoiGian'] = x.NhacTruocLoaiThoiGian
                    });
                    self.tblSetUpThongBao = obj.dataSoure.sort(function (a, b) {
                        let x = a.STT, y = b.STT;
                        return x > y ? 1 : x < y ? -1 : 0;
                    });
                }
            });
            self.requestApi();
        }

        self.chat = $.connection.AlertHub;
        //self.chat = $.connection.chatHub;

        self.connected = false;
        $.connection.hub.start().done(function () {
            self.connected = true;
        });

        $.connection.hub.disconnected(function () {
            self.connected = false;
            setTimeout(function () {
                $.connection.hub.start().done(function () {
                    self.connected = true;
                    console.log('connect again hub _vmThongBao');
                });;
            }, 5000);
        });
    },
    data: {
        timeRequest: 2000,
    },
    methods: {
        UpdateThongBao_CongViecDaXuLy: function (param) {
            let obj = {
                ID_NguoiDung: param.ID_NguoiDung,
                ID_PhieuTiepNhan: param.ID_PhieuTiepNhan,
                BienSo: param.BienSo,
                LoaiNhac: param.LoaiNhac,
            }
            ajaxHelper('/api/DanhMuc/HT_NguoiDungAPI/' + 'UpdateThongBao_CongViecDaXuLy', 'POST', obj).done(function (x) {

            })
        },
        Create_tblRequest: function (param) {
            let self = this;
            let lcRequest = localStorage.getItem('lcRequest');

            // check tblSetUp
            let setup = $.grep(self.tblSetUpThongBao, (x) => {
                return x.ID_QuyTrinhTruoc === param.ID_QuyTrinhTruoc
                    && x.ID_QuyTrinhSau === param.ID_QuyTrinhSau;
            });
            if (setup.length > 0) {

                let loaiTG = setup[0].LoaiThoiGian;
                let minutes_Setup = setup[0].ThoiGian;
                switch (loaiTG) {
                    case 2:
                        minutes_Setup = minutes_Setup * 60;
                        break;
                    default:
                        break;
                }

                let obj = {
                    ID_DonVi: param.ID_DonVi,
                    ID_PhieuTiepNhan: param.ID_PhieuTiepNhan,
                    ID_Xe: param.ID_Xe,
                    BienSo: param.BienSo,
                    ThoiGian: param.ThoiGian,
                    ID_QuyTrinhTruoc: param.ID_QuyTrinhTruoc,
                    LoaiNhac: param.ID_QuyTrinhSau,
                    TimeSetup: minutes_Setup,
                    TrangThaiRequest: 0
                };

                if (commonStatisJs.CheckNull(lcRequest)) {
                    lcRequest = [];
                }
                else {
                    lcRequest = JSON.parse(lcRequest);
                }

                let ex = $.grep(lcRequest, (x) => {
                    return x.ID_PhieuTiepNhan === obj.ID_PhieuTiepNhan && x.LoaiNhac === obj.LoaiNhac;
                });
                if (ex.length === 0) {
                    lcRequest.push(obj);
                    localStorage.setItem('lcRequest', JSON.stringify(lcRequest));
                }
                self.requestApi();

                //let hostUrl = "https://signalr.open24.vn/";
                //let objSend = {
                //    Subdomain: '0973474985',
                //    Type: 2,
                //    Title:'Hello',
                //    Body :'every body 001',
                //}
                //ajaxHelper(hostUrl + 'api/Message/SendMessage', 'POST', objSend).done(function (x) {
                //    console.log('xx ', x)
                //})

                //let objPost = {
                //    Subdomain: '0973474985',
                //    UserName: 'admin',
                //    Type: 3, // 1- lịch hẹn, 2- Công việc, 3- Nhắc bảo hành gara
                //    IdThongBao: '1583AD19-0988-47C5-A8C1-B6A13F340239',
                //    //ThoiGianThongBao: moment(new Date()).format('YYYY-MM-DD HH:mm'),
                //    KieuLap: 0,
                //    GiaTriLap: 0,
                //    NoiDungThongBao: 'Xe 01236999 chưa tạo hóa đơn',
                //    TieuDe: 'Báo giá',
                //    Url: '',
                //}
                //// post notifiy
                //ajaxHelper(hostUrl + 'api/Message/PostTest', 'POST', objPost).done(function (x) {
                //    console.log('xx ', x)
                //})
            }
        },

        GetThongBao: async function (param) {
            let xx = await ajaxHelper('/api/DanhMuc/GaraAPI/' + 'ThongBao_TienDoCongViec', 'POST', param).done().then(function (obj) {
                if (obj.res) {
                    return obj.dataSoure;
                }
                return false;
            });
            return xx;
        },
        requestApi: async function () {
            let self = this;

            let lcRequest = localStorage.getItem('lcRequest');
            if (!commonStatisJs.CheckNull(lcRequest)) {
                lcRequest = JSON.parse(lcRequest);

                let arrWait = [];
                for (let i = 0; i < lcRequest.length; i++) {

                    let itFor = lcRequest[i];
                    if (itFor.TrangThaiRequest === 0) {

                        let diff = (new Date() - new Date(itFor.ThoiGian)) / 1000;
                        let minutes = Math.floor(diff / 60);

                        if (minutes >= itFor.TimeSetup) {
                            itFor.TrangThaiRequest = 1;

                            let result = await self.GetThongBao(itFor);
                            console.log('result ', result, lcRequest[i]);
                            if (result) {
                                self.chat.server.hello();
                            }
                        }
                        else {
                            arrWait.push(itFor);
                        }
                    }
                }

                if (arrWait.length == 0) {
                    clearTimeout(self.timeRequest);
                    localStorage.removeItem('lcRequest')
                }
                else {
                    localStorage.setItem('lcRequest', JSON.stringify(arrWait));

                    let diff = (new Date() - new Date(arrWait[0].ThoiGian)) / 1000;
                    let minutes = Math.floor(diff / 60);

                    let time = (arrWait[0].TimeSetup - minutes) * 60000; //1phut = 60000 miliseconds
                    self.timeRequest = setTimeout(self.requestApi, time);
                }
            }
            clearTimeout(self.timeRequest);
        },
    }
});

vmThongBao.chat.client.hello = function () {
    if (vmThongBao.hasHeader) {
        VHeader.GetThongBao();
    }
}