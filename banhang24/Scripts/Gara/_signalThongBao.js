var chat = $.connection.chatHub;

var vmThongBao = new Vue({
    created: function () {
        let self = this;
        self.tblSetUpThongBao = [
            { STT: 1, ID_QuyTrinhTruoc: 0, ID_QuyTrinhSau: 1, ThoiGian: 5, LoaiThoiGian: 1 },
            { STT: 2, ID_QuyTrinhTruoc: 1, ID_QuyTrinhSau: 2, ThoiGian: 10, LoaiThoiGian: 1 },
            { STT: 3, ID_QuyTrinhTruoc: 2, ID_QuyTrinhSau: 3, ThoiGian: 15, LoaiThoiGian: 1 },
        ];
        self.requestApi();
    },
    data: {
        timeRequest: null,
    },
    methods: {
        Create_tblRequest: function (param) {
            let self = this;
            let lcRequest = localStorage.getItem('lcRequest');

            // check tblSetUp
            let setup = $.grep(self.tblSetUpThongBao, (x) => {
                return x.ID_QuyTrinhTruoc === 0;// 0.PTN
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
                    NgayVaoXuong: param.NgayVaoXuong,
                    LoaiNhac: 1,
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
            }
        },

        GetThongBao: async function (param) {
            let xx = await ajaxHelper('/api/DanhMuc/GaraAPI/' + 'ThongBao_TienDoCongViec', 'POST', param).done().then((obj) => {
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

                for (let i = 0; i < lcRequest.length; i++) {
                    let itFor = lcRequest[i];
                    if (itFor.TrangThaiRequest === 0) {

                        let diff = (new Date() - new Date(itFor.NgayVaoXuong)) / 1000;
                        let minutes = Math.floor(diff / 60);
                        if (minutes >= itFor.TimeSetup) {
                            itFor.TrangThaiRequest = 1;

                            let result = await self.GetThongBao(itFor);
                            console.log('result ', result, lcRequest[i]);
                            if (result) {
                                $.connection.hub.start().done(function () {
                                    chat.server.send();
                                });
                            }
                        }
                    }
                }
                console.log('remove');
                lcRequest = $.grep(lcRequest, (x) => {
                    return x.TrangThaiRequest === 0;
                });
                localStorage.setItem('lcRequest', JSON.stringify(lcRequest));

                if (lcRequest.length == 0) {
                    clearTimeout(self.timeRequest);
                }
                else {
                    self.timeRequest = setTimeout(self.requestApi(), 2000);
                }
            }
        },
    }
});

//$(() => {
//    let t = 0;
//    async function requestApi() {
//        let lcRequest = localStorage.getItem('lcRequest');
//        if (!commonStatisJs.CheckNull(lcRequest)) {
//            lcRequest = JSON.parse(lcRequest);

//            for (let i = 0; i < lcRequest.length; i++) {
//                let itFor = lcRequest[i];
//                if (itFor.TrangThaiRequest === 0) {

//                    let diff = (new Date() - new Date(itFor.NgayVaoXuong)) / 1000;
//                    let minutes = Math.floor(diff / 60);
//                    if (minutes >= itFor.TimeSetup) {
//                        itFor.TrangThaiRequest = 1;

//                        let result = await vmThongBao.GetThongBao(itFor);
//                        console.log('result ', result, lcRequest[i]);
//                        if (result) {
//                            $.connection.hub.start().done(function () {
//                                chat.server.send();
//                            });
//                        }
//                    }
//                }
//            }
//            console.log('remove');
//            lcRequest = $.grep(lcRequest, (x) => {
//                return x.TrangThaiRequest === 0;
//            });
//            localStorage.setItem('lcRequest', JSON.stringify(lcRequest));

//            if (lcRequest.length == 0) {
//                clearTimeout(t);
//            }
//            else {
//                t = setTimeout(requestApi, 1000);
//            }
//        }
//    }

//    requestApi();
//})
