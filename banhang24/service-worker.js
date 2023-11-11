// Set this to true for production
//https://github.com/michaelcostabr/aspnet-mvc-pwa


// Name our cache
var CACHE_NAME = 'banle-appcache-v1';

var version = '?v=1.0000125';

var urlsBanLeToCache = [
    '../../manifest.manifest',

    '/Content/BanLeCss' + version,
    '/Content/Open24Css' + version,
    '/Content/CssFramework'+ version,
    '/bundles/JsFramework?v=tFsnefTtgWNyJMzkPiCCkIEzOYP_L5RA4667h4BDVOo1',
    '/bundles/JsStatic?v=Dd2kG_mkfYiS8qizAAuPuSwdys7lGSusFlE2ExPViUc1',

    '/Content/partial.css',
    '/Content/Banhang.css',
    '/Content/ssoftvn.css',
    '/Content/style.css',
    '/Content/responsive.css',

    '/Content/VariablesStyle.css',
    '/Content/printJS/print.min.css',
    '/Content/js/Datetime/jquery.datetimepicker.css',

    //'/Content/js/Open24FileManager.js',
    '/signalr/hubs',

    '/$/banle',

    '/bundles/BanLe' + version,

    '/Scripts/Components/Ko-component.js',
    '/Scripts/Components/Input.js' + version,
    '/Scripts/Components/NhanVien_KhachHang.js' + version,
    '/Scripts/BanHang/ssoft-server-time.js',

    '/Scripts/Gara/_ThongTinThanhToanKH.js' + version,

    '/Scripts/Gara/HoaDon/_HoaHongNhanVien.js' + version,

    '/Scripts/Gara/KhachHang/_ThemNhomKhachHang.js' + version,

    '/Scripts/Components/dropdown-search.js' + version,
    '/Scripts/Components/TrangThai_NguonKhach.js' + version,
    '/Scripts/Gara/KhachHang/_ThemMoiKhachHang.js' + version,

    '/Scripts/Gara/KhachHang/vmTrangThai_NguonKhach.js' + version,

    '/Scripts/Gara/HoaDon/_ThanhToan.js' + version,
    '/Scripts/Gara/HoaDon/_EditHoaHongDV.js' + version,
    '/Scripts/Components/page-list.js' + version,

    '/Content/Treeview/gijgo.js',
    '/Scripts/jquery.cookie.js',
    '/Scripts/ThietLap/MauInTeamplate.js',
    '/Scripts/knockout-jqAutocomplete.min.js',

    '/Content/images/banhang24.png',
    '/Content/images/iconbepp18.9/timkiem.png',
    '/Content/images/bill-icon/Giaodien24hh-26.png',
    '/Content/images/iconbepp18.9/dongbodulieu.png',
    '/Content/images/iconbepp18.9/tuychon.png',
    '/Content/images/bill-icon/Giaodien24hh-29.png',
    '/Content/images/bill-icon/Giaodien24hh-30.png',
    '/Content/images/bill-icon/Giaodien24hh-31.png',
    '/Content/images/bill-icon/Giaodien24hh-33.png',
    '/Content/images/bill-icon/Giaodien24hh-32.png',
    '/Content/images/bill-icon/Giaodien24hh-34.png',
    '/Content/images/bill-icon/up1.png',
    '/Content/images/open24.vn.png',
    '/Content/images/bill-icon/Giaodien24hh-24.png',
    '/Content/images/iconketchen/m4.png',
    '/Content/images/icon/ngaysinh.png',
    '/Content/images/icon/Iconthemmoi-16.png',
    '/Content/images/iconbepp18.9/innhap.png',
    '/Content/images/iconbepp18.9/guiggmail.png',
    '/Content/images/print/icccoo-11.png',
    '/Content/images/iconbepp18.9/xtat.png',
    '/Content/images/icon/Iconthemmoi-14.png',
    '/Content/images/iconbepp18.9/gg-37.png',
    '/Content/images/wait.gif',
    '/Content/images/anhhh/tracuutonkhooo-49.png',
    '/Content/images/icon/right2.png',
    '/Content/images/print/nhin.png',
    '/Content/images/khuyenmai.png ',
    '/Content/images/giamgia.png',
    '/Content/images/photo.png',
    '/Content/images/icon/Iconthemmoi-17.png',
    '/Content/images/anhhh/print.png',
    '/Content/images/anhhh/logo24.png ',
    '/Content/images/anhhh/place.png ',
    '/Content/images/icon/gioi-tinh-nam.png ',
    '/Content/images/icon/gioi-tinh-nu.png',
    '/Content/images/icon/services.png',
    '/Content/images/icon/discount.png',
    '/Content/images/icon/nhanvien-thuchien.png',
    '/Content/images/icon/nhanvien-tu-van.png',
    '/Content/images/icon/user-discount-1.png',
    '/Content/images/icon/user-discount.png',
    '/Content/images/icon/icon-folder.png',
    '/Content/images/icon/nhat-ky-dich-vu.png',
    '/Content/images/nhahang/vi-tri.png',
    '/Content/images/logo-open24-min.png',
    '/Content/images/print-min.png ',
    '/Content/images/coner.png ',
    '/Content/images/nhin-min.png ',
    '/Content/images/hotro/iconhotro-10.png',
    '/Content/images/hotro/iconhotro-11.png',
    '/Content/images/hotro/iconhotro-12.png',
    '/Content/images/conner-white.png'
];

// Delete old caches that are not our current one!
self.addEventListener("activate", event => {
    const cacheWhitelist = [CACHE_NAME];
    event.waitUntil(
        caches.keys()
            .then(keyList =>
                Promise.all(keyList.map(key => {
                    if (!cacheWhitelist.includes(key)) {
                        console.log('Deleting cache: ' + key);
                        return caches.delete(key);
                    }
                }))
            )
    );
});

// The first time the user starts up the PWA, 'install' is triggered.
self.addEventListener('install', event => {
    event.waitUntil((async () => {
        const cache = await caches.open(CACHE_NAME);
        cache.addAll(urlsBanLeToCache);
    })());
});

// When the webpage goes to fetch files, we intercept that request and serve up the matching files
// if we have them
self.addEventListener('fetch', function (event) {
    event.respondWith(
        // caches.match: bộ nhớ đệm
        caches.match(event.request)
            .then(function (response) {

                let req = event.request;
                //console.log('req ', req)
                // Cache hit - return response
                if (response) {
                    return response;
                }
               
                if (req.url.indexOf('signalr') > -1) {
                    return response;
                }
                // if not found --> get from network
                return fetch(req).then(function (response) {
                    // Check if we received a valid response
                    if (!response || response.status !== 200 || response.type !== 'basic') {
                        return response;
                    }
                    // copy and set to cache & brower
                    var responseToCache = response.clone();

                    caches.open(CACHE_NAME)
                        .then(function (cache) {
                            //console.log('event.request ', event.request, responseToCache)
                            if (req.method === "POST") {
                                // save to indexDB (because can't cache POST requests to  Cache API)
                            }
                            else {
                                cache.put(req, responseToCache);
                            }
                        });
                    return response;
                });
            })
    );
});
