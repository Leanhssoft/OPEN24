var vmThongBao = new Vue({
    created: function () {
        let self = this;
        self.tblSetUpThongBao = [
            { STT: 1, ID_QuyTrinhTruoc: 0, ID_QuyTrinhSau: 1, ThoiGian: 5, LoaiThoiGian: 1 },
            { STT: 2, ID_QuyTrinhTruoc: 1, ID_QuyTrinhSau: 2, ThoiGian: 10, LoaiThoiGian: 1 },
            { STT: 3, ID_QuyTrinhTruoc: 2, ID_QuyTrinhSau: 3, ThoiGian: 15, LoaiThoiGian: 1 },
            { STT: 4, ID_QuyTrinhTruoc: 3, ID_QuyTrinhSau: 4, ThoiGian: 15, LoaiThoiGian: 1 },
            { STT: 5, ID_QuyTrinhTruoc: 4, ID_QuyTrinhSau: 5, ThoiGian: 15, LoaiThoiGian: 1 },
            { STT: 6, ID_QuyTrinhTruoc: 5, ID_QuyTrinhSau: 6, ThoiGian: 15, LoaiThoiGian: 1 },
        ];
        self.requestApi();

        self.chat = $.connection.AlertHub;
        //self.chat = $.connection.chatHub;
    },
    data: {
        timeRequest: 1000,
    },
    methods: {
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
                debugger

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
            }
        },

        GetThongBao: async function (param) {
            let xx = await ajaxHelper('/api/DanhMuc/GaraAPI/' + 'ThongBao_TienDoCongViec', 'POST', param).done().then(function (obj){
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
                    debugger

                    let itFor = lcRequest[i];
                    if (itFor.TrangThaiRequest === 0) {

                        let diff = (new Date() - new Date(itFor.ThoiGian)) / 1000;
                        let minutes = Math.floor(diff / 60);

                        if (minutes >= itFor.TimeSetup) {
                            itFor.TrangThaiRequest = 1;

                            let result = await self.GetThongBao(itFor);
                            console.log('result ', result, lcRequest[i]);
                            if (result) {
                                $.connection.hub.start().done(function () {
                                    self.chat.server.hello();
                                });
                            }
                        }
                        else {
                            arrWait.push(itFor);
                        }
                    }
                }
                console.log('lcRequest ', arrWait.length);

                if (arrWait.length == 0) {
                    clearTimeout(self.timeRequest);
                    localStorage.removeItem('lcRequest')
                }
                else {
                    localStorage.setItem('lcRequest', JSON.stringify(arrWait));

                    let diff = (new Date() - new Date(arrWait[0].ThoiGian)) / 1000;
                    let minutes = Math.floor(diff / 60);

                    setTimeout(self.requestApi(), (arrWait[0].TimeSetup - minutes) * 60000)
                    //self.timeRequest = setTimeout(self.requestApi(), (arrWait[0].TimeSetup - minutes) * 60000);// 1phut = 60000 miliseconds
                }
            }
        },
    }
});