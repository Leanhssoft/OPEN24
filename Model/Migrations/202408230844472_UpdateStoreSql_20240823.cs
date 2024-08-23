namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class UpdateStoreSql_20240823 : DbMigration
    {
        public override void Up()
        {
            Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhMonth_DoanhThuBanHang]
	@year [int] = 2024,
    @ID_ChiNhanh [nvarchar](max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de'
AS
BEGIN
	SET NOCOUNT ON;
    SELECT
    	a.ThangLapHoaDon,
    	CAST(ROUND(SUM(a.DoanhThu), 0) as float) as DoanhThu,
		CAST(ROUND(SUM(a.GiaVonGDV), 0) as float) as GiaVonGDV,
    	CAST(ROUND(SUM(a.GiaTriTra), 0) as float) as GiaTriTra,
    	CAST(ROUND(SUM(a.GiamGiaHDB - a.GiamGiaHDT), 0) as float) as GiamGiaHD
    	FROM
    	(
    		Select 
    		DATEPART(MONTH, hd.NgayLapHoaDon) as ThangLapHoaDon,
    		hd.LoaiHoaDon,
    		Case When hd.LoaiHoaDon in (1,19,25) 
				---- không lấy tpdl, tp combo (tp con) --
				and (hdct.ID_ChiTietDinhLuong is null or hdct.ID_ChiTietDinhLuong = hdct.ID) then hdct.ThanhTien else 0 end as DoanhThu,
				---- không lấy DV chứa tpdl (DV cha) --
			Case When hd.LoaiHoaDon = 1 and hdct.ChatLieu = 4 and (hdct.ID_ChiTietDinhLuong is null or hdct.ID_ChiTietDinhLuong != hdct.ID)
				then ISNULL(hdct.SoLuong * hdct.GiaVon ,0) else 0 end as GiaVonGDV,
    		Case When hd.LoaiHoaDon = 6 then hdct.ThanhTien else 0 end as GiaTriTra,
			Case when hd.TongTienHang != 0 and hd.LoaiHoaDon in (1,19,25) 
				then hdct.ThanhTien * ((ISNULL(hd.TongGiamGia, 0) + ISNULL(hd.KhuyeMai_GiamGia, 0)) / ISNULL(hd.TongTienHang, 0)) else 0 end as GiamGiaHDB,
			Case when hd.TongTienHang != 0 and hd.LoaiHoaDon = 6 
				then hdct.ThanhTien * ((ISNULL(hd.TongGiamGia, 0) + ISNULL(hd.KhuyeMai_GiamGia, 0)) / ISNULL(hd.TongTienHang, 0)) else 0 end as GiamGiaHDT
    		From BH_HoaDon hd
			join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon 
    		where hd.LoaiHoaDon in (1,19,25,6)
    		and DATEPART(YEAR, hd.NgayLapHoaDon) = @year
    		and hd.ChoThanhToan = 0
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh)) 
			and (hdct.ChatLieu is null or hdct.ChatLieu!= 5) -- 5.bi huy
    	) as a
    	GROUP BY
    	a.ThangLapHoaDon
END");
            Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_TheoNhanVien]
    @TenNhanVien [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang uniqueidentifier,
	@LoaiChungTu [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@IDPhongBan nvarchar(max)
AS
BEGIN

		-- Tạo biến lưu trữ kết quả từ hàm GetIDNhanVien_inPhongBan
		DECLARE @tblNhanVien TABLE (ID UNIQUEIDENTIFIER);

		-- Lấy danh sách nhân viên có thể xem dựa trên quyền người dùng
		INSERT INTO @tblNhanVien
		select ID from dbo.GetIDNhanVien_inPhongBan(@ID_NguoiDung, @ID_ChiNhanh,'BCBH_TheoNhanVien_XemDS_PhongBan','BCBH_TheoNhanVien_XemDS_HeThong');
	
		DECLARE @tblChiNhanh TABLE (ID_DonVi uniqueidentifier)
		insert into @tblChiNhanh
		select * from splitstring(@ID_ChiNhanh)

		DECLARE @tblNhomHang TABLE (ID uniqueidentifier)
		insert into @tblNhomHang
		SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)

		DECLARE @tblLoaiChungTu TABLE(ID INT)
		INSERT INTO @tblLoaiChungTu
		select Name from splitstring(@LoaiChungTu);


		DECLARE @tblDepartment TABLE (ID_PhongBan uniqueidentifier)
		if @IDPhongBan =''
			insert into @tblDepartment
			select distinct ID_PhongBan from NS_QuaTrinhCongTac pb
		else
			insert into @tblDepartment
			select * from splitstring(@IDPhongBan)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TenNhanVien, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);

	declare @tblCTHD table (
		NgayLapHoaDon datetime,
		MaHoaDon nvarchar(max),
		LoaiHoaDon int,
		ID_DonVi uniqueidentifier,
		ID_PhieuTiepNhan uniqueidentifier,
		ID_DoiTuong uniqueidentifier,
		ID_NhanVien uniqueidentifier,
		TongTienHang float,
		TongGiamGia	float,
		KhuyeMai_GiamGia float,
		ChoThanhToan bit,
		ID uniqueidentifier,
		ID_HoaDon uniqueidentifier,
		ID_DonViQuiDoi uniqueidentifier,
		ID_LoHang uniqueidentifier,
		ID_ChiTietGoiDV	uniqueidentifier,
		ID_ChiTietDinhLuong uniqueidentifier,
		ID_ParentCombo uniqueidentifier,
		SoLuong float,
		DonGia float,
		GiaVonfloat float,
		TienChietKhau float,
		TienChiPhi float,
		ThanhTien float,
		ThanhToan float,
		GhiChu nvarchar(max),
		ChatLieu nvarchar(max),
		LoaiThoiGianBH int,
		ThoiGianBaoHanh float,
		TenHangHoaThayThe nvarchar(max),
		TienThue float,	
		GiamGiaHD float,
		GiamTruThanhToanBaoHiem float,
		GiaVon float,
		TienVon float
		)
	insert into @tblCTHD
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

	declare @tblChiPhi table (ID_ParentCombo uniqueidentifier,ID_DonViQuiDoi uniqueidentifier, ChiPhi float, 
		ID_NhanVien uniqueidentifier,ID_DoiTuong uniqueidentifier)
	insert into @tblChiPhi
	exec BCBanHang_GetChiPhi @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

    SELECT 
    	a.ID_NhanVien,
		a.MaNhanVien,
    	a.TenNhanVien, 
		a.SoLuongMua as SoLuongBan,
		cast(a.SoLuongTra as float) as SoLuongTra,
		a.GiaTriMua as ThanhTien,
		cast(a.GiaTriTra as float) as GiaTriTra,
		a.GiamGiaHangMua - a.GiamGiaHangTra as GiamGiaHD,
		a.GiamTruThanhToanBaoHiem,
		a.TongThueHangMua - a.TongThueHangTra as TienThue,
		isnull(cpOut.ChiPhi,0) as ChiPhi,
		a.GiaVonHangMua - a.GiaVonHangTra as TienVon,
		a.GiaTriMua - a.GiaTriTra  - (a.GiamGiaHangMua + a.GiamTruThanhToanBaoHiem - a.GiamGiaHangTra) as DoanhThu,
		a.GiaTriMua - a.GiaTriTra  - (a.GiamGiaHangMua - a.GiamTruThanhToanBaoHiem - a.GiamGiaHangTra) - (a.GiaVonHangMua - a.GiaVonHangTra)-isnull(cpOut.ChiPhi,0) as LaiLo		
    	FROM
    	(

		select 
		tblMuaTra.ID_NhanVien, 
		nv.TenNhanVien, nv.MaNhanVien,
		sum(SoLuongMua  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongMua,
		sum(GiaTriMua) as GiaTriMua,
		sum(TongThueHangMua) as TongThueHangMua,
		sum(GiamGiaHangMua) as GiamGiaHangMua,
		sum(GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
		sum(GiaVonHangMua) as GiaVonHangMua,
		sum(SoLuongTra  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongTra,
		sum(GiaTriTra) as GiaTriTra,
		sum(TongThueHangTra) as TongThueHangTra,
		sum(GiamGiaHangTra) as GiamGiaHangTra,
		sum(GiaVonHangTra) as GiaVonHangTra
	from
		(		
		---- doanhthu + giavon hd le
		
		select 
			ct.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang,			
			sum(SoLuong) as SoLuongMua,
			sum(ThanhTien) as GiaTriMua,
			sum(ct.TienThue) as TongThueHangMua,
			sum(ct.GiamGiaHD) as GiamGiaHangMua,
			sum(ct.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
			sum(ct.TienVon)	as GiaVonHangMua,			
			0 as SoLuongTra,
			0 as GiaTriTra,
			0 as TongThueHangTra,
			0 as GiamGiaHangTra,
			0 as GiaVonHangTra
		from @tblCTHD ct			
		where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)		
		group by ct.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang

			---- giatritra + giavon hangtra

		union all
		select 
			hd.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang,
			0 as SoLuongMua,
			0 as GiaTriMua,
			0 as TongThueHangMua,
			0 as GiamGiaHangMua,
			0 as GiamTruThanhToanBaoHiem,
			0 as GiaVonHangMua,
			sum(SoLuong) as SoLuongTra,
			sum(ThanhTien) as GiaTriTra,
			sum(ct.TienThue * ct.SoLuong) as TienThueHangTra,
			sum(iif(hd.TongTienHang=0,0, ct.ThanhTien  * hd.TongGiamGia /hd.TongTienHang)) as GiamGiaHangTra,
			sum(ct.SoLuong * ct.GiaVon) as GiaVonHangTra
		from BH_HoaDon hd		
		join BH_HoaDon_ChiTiet ct on hd.id= ct.ID_HoaDon
		where hd.ChoThanhToan= 0
		and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)
		and hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd
		and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
		and exists (select Name from dbo.splitstring(@LoaiChungTu) ctu where hd.LoaiHoaDon= ctu.Name)
		and hd.LoaiHoaDon =6
		and (ct.ChatLieu is null or ct.ChatLieu !='4') ---- khong lay ct sudung dichvu
		group by hd.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang
	) tblMuaTra 
	join NS_NhanVien nv on tblMuaTra.ID_NhanVien= nv.ID
	join DonViQuiDoi qd on tblMuaTra.ID_DonViQuiDoi= qd.ID
	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
	left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
		where 
		exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)
		and iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) in (select name from dbo.splitstring(@LoaiHangHoa))		
    	and hh.TheoDoi like @TheoDoi
		and qd.Xoa like @TrangThai		
	    AND EXISTS (SELECT pb.ID_PhongBan
                       FROM @tblDepartment pb 
                       JOIN NS_QuaTrinhCongTac ct ON pb.ID_PhongBan = ct.ID_PhongBan OR ct.ID_PhongBan IS NULL
                        WHERE ct.ID_NhanVien = nv.ID)
		AND(EXISTS (SELECT ID FROM @tblNhanVien nv2 WHERE nv2.ID = nv.ID))		
		AND
		((select count(Name) from @tblSearchString b where 
			nv.TenNhanVien like '%'+b.Name+'%' 
    			or nv.TenNhanVienKhongDau like '%'+b.Name+'%' 
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%' +b.Name +'%' 
					or nv.DienThoaiDiDong like '%' +b.Name +'%'
					or nv.DienThoaiNhaRieng like '%' +b.Name +'%'
    		)=@count or @count=0)
		
		group by tblMuaTra.ID_NhanVien, nv.TenNhanVien, nv.MaNhanVien
    	) a
		left join (
			select cp.ID_NhanVien, sum(cp.ChiPhi) as Chiphi
			from @tblChiPhi cp
			group by cp.ID_NhanVien
		) cpOut on a.ID_NhanVien= cpOut.ID_NhanVien
END");
            Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhMonth_GiaVonBanHang]
	@year [int] ='2024',
    @ID_ChiNhanh [nvarchar](max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de'
AS
BEGIN
SET NOCOUNT ON;
DECLARE @tblThang TABLE (ThangLapHoaDon INT, TongGiaVonBan FLOAT, TongGiaVonTra FLOAT);
INSERT INTO @tblThang (ThangLapHoaDon, TongGiaVonBan, TongGiaVonTra)
VALUES (1, 0, 0), (2,0,0), (3,0,0), (4,0,0), (5,0,0), (6,0,0), (7,0,0), (8,0,0), (9,0,0), (10,0,0), (11,0,0), (12,0,0);
	SELECT 
		b.ThangLapHoaDon,
		SUM(CAST(ROUND(b.TongGiaVonBan, 0) as float)) as TongGiaVonBan,
		SUM(CAST(ROUND(b.TongGiaVonTra , 0) as float)) as TongGiaVonTra
	FROM
	(
    SELECT
    	a.ThangLapHoaDon,
		sum(a.GiaVonBan) as TongGiaVonBan,
		sum(a.GiaVonTra) as TongGiaVonTra
    	FROM
    	(
			---- hdban ---
    		Select 
    			DATEPART(MONTH, hd.NgayLapHoaDon) as ThangLapHoaDon,
				hdct.SoLuong * ISNULL(iif(hd.LoaiHoaDon =1, hdct.GiaVon,0), 0) as GiaVonBan,
				0 as GiaVonTra
    		From BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon				
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		where hd.LoaiHoaDon in (1,19)
				----- không lấy định lượng ---
				and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
				----- không lấy thành phần combo ---
				and (hdct.ID_ParentCombo = hdct.ID or hdct.ID_ParentCombo is null)
    			and DATEPART(YEAR, hd.NgayLapHoaDon) = @year
    			and hd.ChoThanhToan = 0
    			and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))

			Union all

			--- hdtra ---
			Select 
    			DATEPART(MONTH, hdt.NgayLapHoaDon) as ThangLapHoaDon,			
				0 as GiaVonBan,
				hdct.SoLuong * ISNULL(hdct.GiaVon, 0) as GiaVonTra    			
    		From BH_HoaDon hdt		
    		join BH_HoaDon_ChiTiet hdct on hdt.ID = hdct.ID_HoaDon 				
    		where hdt.LoaiHoaDon = 6 
			and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)			
    		and DATEPART(YEAR, hdt.NgayLapHoaDon) = @year
    		and hdt.ChoThanhToan = 0
    		and hdt.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))

    	) as a
    	GROUP BY  a.ThangLapHoaDon
		UNION ALL SELECT * FROM @tblThang
		)
		as b
		
		GROUP BY b.ThangLapHoaDon
END");
			Sql(@"ALTER PROCEDURE [dbo].[UpdateChiTietKiemKe_WhenEditCTHD]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanhInput [uniqueidentifier],
    @NgayLapHDMin [datetime]
AS
BEGIN
    SET NOCOUNT ON;
	  
		 ------- get all donviquydoi lienquan ---
			declare @tblQuyDoi table (ID_DonViQuiDoi uniqueidentifier, ID_HangHoa uniqueidentifier, 
				ID_LoHang uniqueidentifier, 
				TyLeChuyenDoi float,
				LaHangHoa bit)
			insert into @tblQuyDoi
			select * from dbo.fnGetAllHangHoa_NeedUpdateTonKhoGiaVon(@IDHoaDonInput)

			------ get all ctKiemKe need update ---
			DECLARE @cthdNeed TABLE (ID_HoaDon UNIQUEIDENTIFIER, ID_ChiTietHoaDon UNIQUEIDENTIFIER, 
			NgayLapHoaDon datetime,SoLuong float, ID_HangHoa UNIQUEIDENTIFIER,
			ID_LoHang UNIQUEIDENTIFIER, ID_DonViQuiDoi UNIQUEIDENTIFIER, TonDauKy float, TyLeChuyenDoi float, TienChietKhau float)
			insert into @cthdNeed
			select 
				hd.ID as ID_HoaDon,
				ct.ID as ID_ChiTietHoaDon,
				hd.NgayLapHoaDon,
				ct.SoLuong,
				qd.ID_HangHoa,
				ct.ID_LoHang,
				ct.ID_DonViQuiDoi,
				0 as TonDauKy,
				qd.TyLeChuyenDoi,
				ct.TienChietKhau
			from BH_HoaDon hd
			join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
			join  @tblQuyDoi qd on ct.ID_DonViQuiDoi= qd.ID_DonViQuiDoi			
				and (ct.ID_LoHang = qd.ID_LoHang or ct.ID_LoHang is null and qd.ID_LoHang is null)
    		WHERE hd.ChoThanhToan = 0 
			AND hd.LoaiHoaDon = 9 
    		and hd.ID_DonVi = @IDChiNhanhInput 
			and hd.NgayLapHoaDon >= @NgayLapHDMin



				----- get tonLuyKe all cthd LienQuan ----			
					select 
						ID_ChiTietHoaDon,
						MaHoaDon,
						ID_LoHang,
						ID_HangHoa,
						TonLuyKe,
						NgayLapHoaDon
					into #cthdLienQuan
					from
					(
					select 
						ct.ID as ID_ChiTietHoaDon,							
						ct.ID_HoaDon,							
						ct.ID_LoHang,
						qd.ID_HangHoa,			
						hd.MaHoaDon,
						CASE WHEN @IDChiNhanhInput = hd.ID_CheckIn and hd.YeuCau = '4' then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon,
						CASE WHEN @IDChiNhanhInput = hd.ID_CheckIn and hd.YeuCau = '4' THEN ct.TonLuyKe_NhanChuyenHang ELSE ct.TonLuyKe END as TonLuyKe
					from BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd  ON ct.ID_HoaDon = hd.ID  		
					join @tblQuyDoi qd on  ct.ID_DonViQuiDoi = qd.ID_DonViQuiDoi
						and (qd.ID_LoHang = ct.ID_LoHang or (qd.ID_LoHang is null and ct.ID_LoHang is null))
    				WHERE hd.ChoThanhToan = 0    		
						and hd.LoaiHoaDon NOT IN (3, 19, 25,29)					
						and exists (select ctNeed.ID_DonViQuiDoi 
								from @cthdNeed ctNeed 
								where ctNeed.ID_HangHoa = qd.ID_HangHoa 
								and (ctNeed.ID_LoHang = ct.ID_LoHang or (ctNeed.ID_LoHang is null and ct.ID_LoHang is null))
								------ chỉ lấy những hóa đơn có ngày lập < ngày kiểm kê (có thể có nhiều khoảng ngày kiểm kê )---
								AND ((hd.ID_DonVi = @IDChiNhanhInput and hd.NgayLapHoaDon <  ctNeed.NgayLapHoaDon and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    							or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanhInput and  hd.NgaySua < ctNeed.NgayLapHoaDon ))
								)
					)cthdLienQuan
		
			
			

			update ctNeed set ctNeed.TonDauKy = cthd.TonDauKy
				from @cthdNeed ctNeed
				join
				(
					select 
						cthdIn.ID_ChiTietHoaDon,
						cthdIn.TonDauKy
					from
					(
					select ctNeed.ID_ChiTietHoaDon, 
						ctNeed.ID_LoHang,
						ctNeed.ID_HangHoa,
						ctNeed.NgayLapHoaDon,
									
						isnull(tkDK.TonLuyKe,0) as TonDauKy,
						----- Lấy tồn đầu kỳ của từng chi tiết hóa đơn, ưu tiên sắp xếp theo tkDK.NgayLapHoaDon gần nhất (max) ---
						----- vì có thể có nhiều hd < ngaylaphoadon of ctNeed ----
						ROW_NUMBER() over (partition by ctNeed.ID_ChiTietHoaDon order by tkDK.NgayLapHoaDon desc) as RN
					from @cthdNeed ctNeed
					left join #cthdLienQuan tkDK on ctNeed.ID_HangHoa = tkDK.ID_HangHoa 
						and (ctNeed.ID_LoHang = tkDK.ID_LoHang or (ctNeed.ID_LoHang is null and tkDK.ID_LoHang is null)) 
						and tkDK.NgayLapHoaDon < ctNeed.NgayLapHoaDon
					)cthdIn
					where rn = 1
				)cthd on cthd.ID_ChiTietHoaDon = ctNeed.ID_ChiTietHoaDon
					


			---------- update TonkhoDB, SoLuongLech, GiaTriLech to BH_HoaDon_ChiTiet----
			update ctkiemke
			set	ctkiemke.TienChietKhau = ctLast.TonDauKy, 
    			ctkiemke.SoLuong = ctkiemke.ThanhTien - ctLast.TonDauKy, ---- soluonglech
    			ctkiemke.ThanhToan = ctkiemke.GiaVon * (ctkiemke.ThanhTien - ctLast.TonDauKy) --- gtrilech = soluonglech * giavon		
			from BH_HoaDon_ChiTiet ctkiemke
			join  
			(
				select cthd.ID_ChiTietHoaDon, 
					----- phai quydoi TonKho theo dvt ---
					cthd.TonDauKy/cthd.TyLeChuyenDoi as TonDauKy, 
					cthd.ID_HangHoa, cthd.TyLeChuyenDoi
				from @cthdNeed cthd	
				where ROUND(cthd.TonDauKy/ cthd.TyLeChuyenDoi, 3) !=  ROUND(cthd.TienChietKhau, 3) 
			) ctLast on ctkiemke.ID =  ctLast.ID_ChiTietHoaDon


			------------- update TongChenhLech for BH_HoaDon ----
			-------- TongGiamGia: sum(SoLuonglech),
			-------- TongTienThue = sum(GiaTriLech) ---
			-------- TongChiPhi: Tổng số lượng lệch tăng = sum (SoLuong - chỉ lấy SoLuong > 0)
			-------- TongTienHang: Tổng số lượng lệch giảm = sum (SoLuong - chỉ lấy SoLuong < 0)

			update hdKK set 
				hdKK.TongGiamGia = ctKK.SoLuongLech,
				hdKK.TongTienThue = ctKK.GiaTriLech,
				hdKK.TongChiPhi = ctKK.SoLuongLechTang,
				hdKK.TongTienHang = ctKK.SoLuongLechGiam
			from BH_HoaDon hdKK
			join
			(
				select 
					ct.ID_HoaDon,
					sum(ct.SoLuong) as SoLuongLech,
					sum(iif(ct.SoLuong >0, ct.SoLuong,0)) as SoLuongLechTang,
					sum(iif(ct.SoLuong < 0, ct.SoLuong,0)) as SoLuongLechGiam,
					sum(ct.ThanhToan) as GiaTriLech
				from BH_HoaDon_ChiTiet ct
				where exists (select ctNeed.ID_HoaDon from @cthdNeed ctNeed where ctNeed.ID_ChiTietHoaDon = ct.ID)
				group by ct.ID_HoaDon
			)ctKK on hdKK.ID = ctKK.ID_HoaDon		
			
	
			drop table #cthdLienQuan


END");
        }
        
        public override void Down()
        {
        }
    }
}
