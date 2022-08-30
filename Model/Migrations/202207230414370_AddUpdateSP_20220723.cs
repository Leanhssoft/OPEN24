namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20220723 : DbMigration
    {
        public override void Up()
        {
			CreateStoredProcedure(name: "[dbo].[GetQuyHoaDon_byIDHoaDon]", parametersAction: p => new
			{
				ID = p.Guid(),
				ID_Parent = p.Guid()
			}, body: @"SET NOCOUNT ON;

	declare @tblThuDatHang table
		(ID uniqueidentifier, LoaiHoaDon int, MaHoaDon nvarchar(max), NgayLapHoaDon datetime,TongTienThu float,
		NguoiTao nvarchar(max), NguoiSua nvarchar(max), TrangThai bit, PhuongThucTT nvarchar(max))

	declare @tblThuHoaDon table
		(ID uniqueidentifier, LoaiHoaDon int,MaHoaDon nvarchar(max), NgayLapHoaDon datetime,TongTienThu float,
		NguoiTao nvarchar(max), NguoiSua nvarchar(max), TrangThai bit, PhuongThucTT nvarchar(max))


    ----- get hd if was create from hdDatHang
	declare @isFirst bit ='0'
	if ( @ID = (select top 1 hd.ID
		from BH_HoaDon hd where hd.ID_HoaDon= @ID_Parent
		and hd.ChoThanhToan='0'
		order by hd.NgayApDungGoiDV
		))
	begin		
		----- get phieu thu dathang
				insert into @tblThuDatHang		
				select 
						tblMain.ID,
						qhd.LoaiHoaDon,
						qhd.MaHoaDon,
						qhd.NgayLapHoaDon,
						sum(qct.TienThu) as TongTienThu,
						qhd.NguoiTao,
						qhd.NguoiSua,
						qhd.TrangThai,								
						max(Left(tblMain.sPhuongThuc,Len(tblMain.sPhuongThuc)-1)) As PhuongThucTT	
					from
					(
						Select distinct hdXML.ID, 							
						 (
							select distinct (case qct.HinhThucThanhToan
										when 1 then N'Tiền mặt'
										when 2 then N'POS'
										when 3 then N'Chuyển khoản'
										when 4 then N'Thẻ giá trị'
										when 5 then N'Điểm'
										when 6 then iif(qhd.LoaiHoaDon=11, N'Thu từ cọc', N'Chi từ cọc')
									else '' end) +', '  AS [text()]
							from Quy_HoaDon_ChiTiet qct
							join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID						
							where qct.ID_HoaDon = hdXML.ID
							and qct.ID_HoaDonLienQuan = @ID_Parent
							and (qhd.TrangThai is null or qhd.TrangThai='1')
							For XML PATH ('')
						) sPhuongThuc		
				From Quy_HoaDon hdXML				
				) tblMain 
				 left join Quy_HoaDon qhd on tblMain.ID = qhd.ID			
				left join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
				where qct.ID_HoaDonLienQuan = @ID_Parent
				group by tblMain.ID	, 
						qhd.LoaiHoaDon,
						qhd.MaHoaDon,
						qhd.NgayLapHoaDon,		
						qhd.NguoiTao,
						qhd.NguoiSua,
						qhd.TrangThai	
	end


	----- get phieu thu hoadon (thu cua chinh no): lấy cả phiếu thu đã hủy
		insert into @tblThuHoaDon		
		select 
						tblMain.ID,
						qhd.LoaiHoaDon,
						qhd.MaHoaDon,
						qhd.NgayLapHoaDon,
						sum(qct.TienThu) as TongTienThu,
						qhd.NguoiTao,
						qhd.NguoiSua,
						qhd.TrangThai,								
						max(Left(tblMain.sPhuongThuc,Len(tblMain.sPhuongThuc)-1)) As PhuongThucTT	
					from
					(
						Select distinct hdXML.ID, 							
						 (
							select distinct (case qct.HinhThucThanhToan
										when 1 then N'Tiền mặt'
										when 2 then N'POS'
										when 3 then N'Chuyển khoản'
										when 4 then N'Thẻ giá trị'
										when 5 then N'Điểm'
										when 6 then iif(qhd.LoaiHoaDon=11, N'Thu từ cọc', N'Chi từ cọc')
									else '' end) +', '  AS [text()]
							from Quy_HoaDon_ChiTiet qct
							join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID						
							where qct.ID_HoaDon = hdXML.ID
							and qct.ID_HoaDonLienQuan = @ID
							For XML PATH ('')
						) sPhuongThuc		
				From Quy_HoaDon hdXML				
				) tblMain 
				 left join Quy_HoaDon qhd on tblMain.ID = qhd.ID			
				left join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
				where qct.ID_HoaDonLienQuan = @ID
				group by tblMain.ID	,
						qhd.LoaiHoaDon,
						qhd.MaHoaDon,
						qhd.NgayLapHoaDon,		
						qhd.NguoiTao,
						qhd.NguoiSua,
						qhd.TrangThai	
	
		select *,
			iif(tbl.LoaiHoaDon=11, N'Phiếu thu', N'Phiếu chi') as SLoaiHoaDon
		from
		(
		select *
		from @tblThuDatHang thuDH
		union all
		select *
		from @tblThuHoaDon thuHD
		) tbl order by tbl.NgayLapHoaDon desc");

			CreateStoredProcedure(name: "[dbo].[GetInforTheGiaTri_byID]", parametersAction: p => new
			{
				ID = p.Guid()
			}, body: @"SET NOCOUNT ON;

				select  tblThe.ID,
						tblThe.ID_DonVi,
						tblThe.ID_DoiTuong,
						tblThe.ID_NhanVien,
						tblThe.MaHoaDon,
						tblThe.NgayLapHoaDon,
						tblThe.LoaiHoaDon,
						tblThe.TongChiPhi as MucNap,
						tblThe.TongChietKhau as KhuyenMaiVND,
						tblThe.KhuyenMaiPT,
						tblThe.ChietKhauPT,
						tblThe.TongTienHang as ThanhTien,
    					tblThe.TongTienHang as TongTienNap,
    					tblThe.TongTienThue as SoDuSauNap,
    					tblThe.TongGiamGia as ChietKhauVND,
						tblThe.DienGiai,
						tblThe.NguoiTao,
						tblThe.DienGiai as GhiChu,
						tblThe.PhaiThanhToan,
						tblThe.ChoThanhToan,
						ISNULL(soquy.TienMat,0) as TienMat,
    					ISNULL(soquy.TienPOS,0) as TienATM,
    					ISNULL(soquy.TienCK,0) as TienGui,
    					ISNULL(soquy.TienThu,0) as KhachDaTra,
    					dv.TenDonVi,
    					dv.SoDienThoai as DienThoaiChiNhanh,
    					dv.DiaChi as DiaChiChiNhanh,
						dt.MaDoiTuong as MaKhachHang,
						dt.TenDoiTuong as TenKhachHang,
						dt.DienThoai as SoDienThoai,
						dt.DiaChi as DiaChiKhachHang
				from
				(
					select hd.ID, 
						hd.MaHoaDon, 
						hd.LoaiHoaDon,
						hd.NgayLapHoaDon,
						hd.ID_DonVi,
						hd.ID_DoiTuong,
						hd.ID_NhanVien,
						hd.TongGiamGia,
						hd.TongChietKhau,
						hd.TongTienThue,
						hd.TongChiPhi,
						hd.TongTienHang,
						hd.PhaiThanhToan,
						hd.TongThanhToan,
						hd.ChoThanhToan,
						hd.DienGiai,
						hd.NguoiTao,
						iif(hd.TongChiPhi=0,0, hd.TongGiamGia/hd.TongChiPhi * 100) as ChietKhauPT,
						iif(hd.TongChiPhi=0,0, hd.TongChietKhau/hd.TongChiPhi * 100) as KhuyenMaiPT,    				
    					case when hd.ChoThanhToan is null then '10' else '12' end as TrangThai
    				from BH_HoaDon hd    				
					where hd.ID= @ID
				) tblThe
				left join DM_DoiTuong dt on tblThe.ID_DoiTuong = dt.ID
				left join DM_DonVi dv on tblThe.ID_DonVi= dv.ID
				left join NS_NhanVien nv on tblThe.ID_NhanVien= nv.ID
				left join ( select quy.ID_HoaDonLienQuan, 
    					sum(quy.TienThu) as TienThu,
    					sum(quy.TienMat) as TienMat,
    					sum(quy.TienPOS) as TienPOS,
    					sum(quy.TienCK) as TienCK
    				from
    				(
    					select qct.ID_HoaDonLienQuan,
    						iif(qct.HinhThucThanhToan = 1, iif(qhd.LoaiHoaDon=11, qct.TienThu,-qct.TienThu),0) as TienMat,
    						iif(qhd.LoaiHoaDon=11, qct.TienThu,-qct.TienThu) as TienThu,
    						case when tk.TaiKhoanPOS = '1' then iif(qhd.LoaiHoaDon=11, qct.TienThu,-qct.TienThu) else 0 end as TienPOS,
    						case when tk.TaiKhoanPOS = '0' then iif(qhd.LoaiHoaDon=11, qct.TienThu,-qct.TienThu) else 0 end as TienCK
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					left join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang= tk.ID
    					where qct.ID_HoaDonLienQuan= @ID and (qhd.TrangThai= 1 or qhd.TrangThai is null)
    				) quy 
    				group by quy.ID_HoaDonLienQuan) soquy on tblThe.ID= soquy.ID_HoaDonLienQuan");

			CreateStoredProcedure(name: "[dbo].[GetThoiGianHoatDong_v1]", parametersAction: p => new
			{
				TextSearch = p.String(),
				ThoiGianFrom = p.DateTime(),
				ThoiGianTo = p.DateTime(),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;

    -- Insert statements for procedure here
	IF(@PageSize != 0)
	BEGIN
	WITH data_cte AS
	(
		select dmx.ID, dmx.BienSo, SUM(nky.SoGioHoatDong) AS TongThoiGianThucHien, COUNT(nky.Id) AS SoLanThucHien from Gara_Xe_NhatKyHoatDong nky
		INNER JOIN Gara_Xe_PhieuBanGiao pbg ON nky.IdPhieuBanGiao = pbg.Id
		INNER JOIN Gara_DanhMucXe dmx ON pbg.IdXe = dmx.ID
		WHERE
		(nky.ThoiGianHoatDong BETWEEN @ThoiGianFrom AND @ThoiGianTo OR @ThoiGianFrom IS NULL)
		AND dmx.BienSo LIKE '%' + @TextSearch + '%'
		GROUP BY dmx.ID, dmx.BienSo
		), count_cte
		AS (
			select count(Id) as TotalRow,
    			CEILING(COUNT(Id) / CAST(@PageSize as float ))  as TotalPage
    		from data_cte
		)
		SELECT dt.*, ct.* FROM data_cte dt
    		CROSS JOIN count_cte ct
    		ORDER BY dt.BienSo desc
    		OFFSET (@CurrentPage * @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY;
	END
	ELSE
	BEGIN
		SELECT dmx.ID, dmx.BienSo, SUM(nky.SoGioHoatDong) AS TongThoiGianThucHien, COUNT(nky.Id) AS SoLanThucHien,
		0 AS TotalRow, CAST(0 AS FLOAT) AS TotalPage
		FROM Gara_Xe_NhatKyHoatDong nky
		INNER JOIN Gara_Xe_PhieuBanGiao pbg ON nky.IdPhieuBanGiao = pbg.Id
		INNER JOIN Gara_DanhMucXe dmx ON pbg.IdXe = dmx.ID
		WHERE
		(nky.ThoiGianHoatDong BETWEEN @ThoiGianFrom AND @ThoiGianTo OR @ThoiGianFrom IS NULL)
		AND dmx.BienSo LIKE '%' + @TextSearch + '%'
		GROUP BY dmx.ID, dmx.BienSo
		ORDER BY dmx.BienSo
	END");

			CreateStoredProcedure(name: "[dbo].[GetListPhuTungTheoDoi_v1]", parametersAction: p => new
			{
				TextSearch = p.String(),
				TrangThais = p.String(),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;
	declare @intTrangThai INT;
    select @intTrangThai = SUM(CAST(Name AS INT)) from dbo.splitstring(@TrangThais);
    -- Insert statements for procedure here
	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);

	-- Lấy danh sách phụ tùng đang theo dõi
    	DECLARE @tblPhuTungTheoDoi TABLE(Id UNIQUEIDENTIFIER, IdHoaDon UNIQUEIDENTIFIER, IdDonViQuiDoi UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, LoaiHoaDon INT, IdXe UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX),
    	IdHangHoa UNIQUEIDENTIFIER, MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), DinhMucBaoDuong FLOAT, ThoiGianHoatDong FLOAT, ThoiGianConLai FLOAT);
    	INSERT INTO @tblPhuTungTheoDoi
    	select ct.ID, hd.ID, dvqd.ID, hd.NgayLapHoaDon, hd.LoaiHoaDon, hd.ID_Xe, dmx.BienSo, hh.ID AS IdHangHoa, dvqd.MaHangHoa, hh.TenHangHoa, 0, 0, 0 from BH_HoaDon hd
    	INNER JOIN BH_HoaDon_ChiTiet ct ON hd.ID = ct.ID_HoaDon
    	INNER JOIN DonViQuiDoi dvqd ON dvqd.ID = ct.ID_DonViQuiDoi
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa AND dvqd.LaDonViChuan = 1
		INNER JOIN Gara_DanhMucXe dmx ON hd.ID_Xe = dmx.ID
    	WHERE hd.LoaiHoaDon = 29
		AND ((select count(Name) from @tblSearch b where dvqd.MaHangHoa LIKE '%' + b.Name + '%'
		OR hh.TenHangHoa LIKE '%' + b.Name + '%'
		OR hh.TenHangHoa_KhongDau LIKE '%' + b.Name + '%'
		OR hh.TenHangHoa_KyTuDau LIKE '%' + b.Name + '%'
		OR dmx.BienSo LIKE '%' + b.Name + '%') = @count OR @count = 0);

		-- Lấy thời gian của hóa đơn sửa chữa gần nhất
    	DECLARE @tblHoaDonSuaChua TABLE(Id UNIQUEIDENTIFIER, IdHoaDon UNIQUEIDENTIFIER, IdXe UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX), IdHangHoa UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME,
    	MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX));
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT MAX(ct.ID) AS Id, hd.ID, pttd.IdXe, pttd.BienSo, pttd.IdHangHoa, MAX(hd.NgayLapHoaDon) AS NgayLapHoaDon, dvqd.MaHangHoa, hh.TenHangHoa FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN Gara_PhieuTiepNhan ptn ON ptn.ID_Xe = pttd.IdXe
    	INNER JOIN BH_HoaDon hd	ON hd.ID_PhieuTiepNhan = ptn.ID
    	INNER JOIN BH_HoaDon_ChiTiet ct ON hd.ID = ct.ID_HoaDon
    	INNER JOIN DonViQuiDoi dvqd ON dvqd.ID = ct.ID_DonViQuiDoi AND dvqd.ID_HangHoa = pttd.IdHangHoa
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
    	WHERE hh.LoaiHangHoa = 1
		AND ((select count(Name) from @tblSearch b where dvqd.MaHangHoa LIKE '%' + b.Name + '%'
		OR hh.TenHangHoa LIKE '%' + b.Name + '%'
		OR hh.TenHangHoa_KhongDau LIKE '%' + b.Name + '%'
		OR hh.TenHangHoa_KyTuDau LIKE '%' + b.Name + '%')= @count OR @count = 0)
    	GROUP BY pttd.IdXe, pttd.IdHangHoa, dvqd.MaHangHoa, hh.TenHangHoa, hd.ID, pttd.BienSo;

		-- Cập nhật thời gian bắt đầu tính thời gian hoạt động dựa theo thời gian hóa đơn sửa chữa gần nhất
    	UPDATE pttd
    	SET pttd.NgayLapHoaDon = hdsc.NgayLapHoaDon, pttd.LoaiHoaDon = 25
    	FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN @tblHoaDonSuaChua hdsc ON pttd.IdXe = hdsc.IdXe AND pttd.IdHangHoa = hdsc.IdHangHoa;
    
    	INSERT INTO @tblPhuTungTheoDoi
    	SELECT hdsc.Id, hdsc.IdHoaDon, dvqd.ID, hdsc.NgayLapHoaDon, 25, hdsc.IdXe, hdsc.BienSo, hdsc.IdHangHoa, hdsc.MaHangHoa, hdsc.TenHangHoa, 0, 0, 0 FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN @tblPhuTungTheoDoi pttd ON hdsc.IdXe = pttd.IdXe AND hdsc.IdHangHoa = pttd.IdHangHoa
    	INNER JOIN DonViQuiDoi dvqd ON dvqd.ID_HangHoa = hdsc.IdHangHoa AND dvqd.LaDonViChuan = 1
    	WHERE pttd.Id IS NULL;

		-- Lấy định mức bảo dưỡng trong danh mục hàng hóa
    	DECLARE @tblDinhMucBaoDuong TABLE (Id UNIQUEIDENTIFIER, DinhMucBaoDuong INT);
    	INSERT INTO @tblDinhMucBaoDuong
    	SELECT pttd.Id, 
    	IIF(bdct.LoaiGiaTri = 1, bdct.GiaTri * 24, 
    	IIF(bdct.LoaiGiaTri = 2, bdct.GiaTri * 24 * 30, 
    	IIF(bdct.LoaiGiaTri = 3, bdct.GiaTri * 24 * 365, 
		IIF(bdct.LoaiGiaTri = 5, bdct.GiaTri, 0)))) AS SoGioDinhMuc FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN DM_HangHoa_BaoDuongChiTiet bdct ON pttd.IdHangHoa = bdct.ID_HangHoa AND bdct.BaoDuongLapDinhKy = 1;
    
    	-- Cập nhật định mức bảo dưỡng
    	UPDATE pttd
    	SET pttd.DinhMucBaoDuong = dmbd.DinhMucBaoDuong
    	FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN @tblDinhMucBaoDuong dmbd ON pttd.Id = dmbd.Id;
    
    	-- Cập nhật thời gian hoạt động và thời gian còn lại
    	UPDATE @tblPhuTungTheoDoi
    	SET ThoiGianHoatDong = ISNULL(dbo.GetSumSoGioHoatDongByIdXe(IdXe, NgayLapHoaDon), 0);
    	UPDATE @tblPhuTungTheoDoi
    	SET ThoiGianConLai = IIF(DinhMucBaoDuong = 0, 0, DinhMucBaoDuong - ThoiGianHoatDong);

		DECLARE @tblResult TABLE(Id UNIQUEIDENTIFIER, IdHoaDon UNIQUEIDENTIFIER, IdDonViQuiDoi UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, LoaiHoaDon INT, IdXe UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX),
    	IdHangHoa UNIQUEIDENTIFIER, MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), DinhMucBaoDuong FLOAT, ThoiGianHoatDong FLOAT, ThoiGianConLai FLOAT);

		INSERT INTO @tblResult
		SELECT * FROM @tblPhuTungTheoDoi
		WHERE (@intTrangThai = 1 AND ThoiGianConLai <= 0) OR (@intTrangThai = 2 AND ThoiGianConLai > 0)
			OR (@intTrangThai = 3);

		DECLARE @TotalRow INT, @TotalPage FLOAT;
		IF(@PageSize != 0)
		BEGIN
			SELECT @TotalRow = COUNT(Id), @TotalPage = CEILING(COUNT(Id) / CAST(@PageSize as float )) FROM @tblResult;
			SELECT *, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblResult
			ORDER BY ThoiGianConLai
			OFFSET (@CurrentPage * @PageSize) ROWS
			FETCH NEXT @PageSize ROWS ONLY;
		END
		ELSE
		BEGIN
			SELECT *, 0 AS TotalRow, CAST(0 AS FLOAT) AS TotalPage FROM @tblResult
			ORDER BY ThoiGianConLai
		END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDichVu_NhatKySuDungChiTiet]
    @Text_Search [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
	@ThoiHan [nvarchar](max),
    @ID_NhomHang UNIQUEIDENTIFIER
AS
BEGIN
	SET NOCOUNT ON;
	declare @dtNow datetime = format(dateadd(day,1, getdate()),'yyyy-MM-dd')

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	declare @tblChiNhanh table (ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh
	select name from dbo.splitstring(@ID_ChiNhanh)

	---- ctsdung ----
select 
	hd.MaHoaDon,
	hd.ID_DoiTuong,
	hd.ID_NhanVien,
	hd.NgayLapHoaDon,
	hd.ID_Xe,
	ct.SoLuong,	
	ct.DonGia,
	ct.GiaVon * ct.SoLuong as TienVon,
	ct.GhiChu,
	ct.ID_DonViQuiDoi,
	ct.ID_LoHang,
	ct.ID_ChiTietDinhLuong,
	ct.ID_ChiTietGoiDV,
	ct.ID
into #tmpCTSD
from BH_HoaDon_ChiTiet ct
join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
where hd.ChoThanhToan = 0
and hd.NgayLapHoaDon between @timeStart and @timeEnd
and exists (select cn.ID_DonVi from @tblChiNhanh cn where cn.ID_DonVi = hd.ID_DonVi)
and hd.LoaiHoaDon in (1,6,25)
and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
and ct.ChatLieu ='4'
order by hd.NgayLapHoaDon desc

---- ctmua ----
select *
into #GDV
from
(
select 
	hd.MaHoaDon,
	hd.TongTienHang,
	hd.TongGiamGia,
	hd.NgayLapHoaDon,
	hd.NgayApDungGoiDV,
	hd.HanSuDungGoiDV,	
	iif(hd.HanSuDungGoiDV is null,'1', iif(@dtNow > hd.HanSuDungGoiDV,'0','1')) as ThoiHan,
	ct.ID,
	ct.DonGia,
	ct.TienChietKhau
from BH_HoaDon_ChiTiet ct
join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
where hd.LoaiHoaDon= 19
and exists (select ctsd.ID from #tmpCTSD ctsd where ctsd.ID_ChiTietGoiDV = ct.ID )
)a 	WHERE a.ThoiHan like @ThoiHan

---- ctxuatkho (hdle or suachua)---
select dluong.ID_ChiTietDinhLuong,
	sum(isnull(dluong.GiaVon,0) * dluong.SoLuong) as TienVon
into #tblGiaVon
from
(
select iif(ct.ID_ChiTietDinhLuong is null, ct.ID,  ct.ID_ChiTietDinhLuong) as ID_ChiTietDinhLuong,
	ct.SoLuong, ct.GiaVon
from BH_HoaDon_ChiTiet ct
where exists (select ctsd.ID from #tmpCTSD ctsd where ctsd.ID = ct.ID_ChiTietDinhLuong )
and ct.ID != ct.ID_ChiTietDinhLuong
) dluong
group by dluong.ID_ChiTietDinhLuong

----- tblview
select *,
CONCAT( b.TenHangHoa , b.ThuocTinh_GiaTri) as TenHangHoaFull
from
(
select 
	ctm.MaHoaDon as MaGoiDV,
	ctsd.MaHoaDon,
	ctsd.NgayLapHoaDon,
	qd.MaHangHoa,
	hh.TenHangHoa,
	qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
	qd.TenDonViTinh,
	lo.MaLoHang as TenLoHang,
	ctsd.SoLuong,
	ctsd.GhiChu,
	ctsd.SoLuong * (ctm.DonGia - ctm.TienChietKhau) * ( 1 -ctm.TongGiamGia/iif(ctm.TongTienHang =0,1,ctm.TongTienHang))  as GiaTriSD,
	--ctsd.SoLuong * ctsd.DonGia as GiaTriSD,
	ceiling(iif(hh.LaHangHoa ='1', ctsd.TienVon, isnull(gv.TienVon,0))) as TienVon,
	dt.MaDoiTuong as MaKhachHang,
	dt.TenDoiTuong as TenKhachHang,
	iif(dt.GioiTinhNam='1', N'Nam', N'Nữ') as GioiTinh,
	dt.DienThoai,
	gt.TenDoiTuong as NguoiGioiThieu,
	nk.TenNguonKhach,
	iif(dt.TenNhomDoiTuongs is null or dt.TenNhomDoiTuongs='',N'Nhóm mặc định', dt.TenNhomDoiTuongs) as NhomKhachHang,
	iif(hh.ID_NhomHang is null, '00000000-0000-0000-0000-000000000000', hh.ID_NhomHang)  as ID_NhomHang,
	nhh.TenNhomHangHoa as TenNhomHang,
	ISNULL(nv.NhanVienChietKhau,'') as NhanVienChietKhau,
	xe.BienSo,
	chuxe.MaDoiTuong as MaChuXe,
	chuxe.TenDoiTuong as TenChuXe
from #tmpCTSD ctsd
left join #tblGiaVon gv on ctsd.ID = gv.ID_ChiTietDinhLuong
left join #GDV ctm on ctsd.ID_ChiTietGoiDV = ctm.ID
join DonViQuiDoi qd on ctsd.ID_DonViQuiDoi= qd.ID
left join DM_LoHang lo on ctsd.ID_LoHang= lo.ID
left join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID
left join DM_DoiTuong dt on ctsd.ID_DoiTuong= dt.ID
left join DM_DoiTuong gt on dt.ID_NguoiGioiThieu = gt.ID
left join DM_NguonKhachHang nk on dt.ID_NguonKhach = nk.ID
left join Gara_DanhMucXe xe on ctsd.ID_Xe= xe.ID
left join DM_DoiTuong chuxe on xe.ID_KhachHang= chuxe.ID
left join (Select Main.ID_ChiTietHoaDon,
							Main.nhanvienchietkhau,
							Main.nhanvienchietkhau_khongdau,
							Main.nhanvienchietkhau_chucaidau    		
    						From
    						(
    						Select distinct hh_tt.ID_ChiTietHoaDon,
    						(
    							Select tt.TenNhanVien + ' - ' AS [text()]
    							From (select nvth.ID_ChiTietHoaDon, nv.TenNhanVien from BH_NhanVienThucHien nvth 
    							inner join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID) tt
    							Where tt.ID_ChiTietHoaDon = hh_tt.ID_ChiTietHoaDon
    							For XML PATH ('')
    						) nhanvienchietkhau,
							(
    							Select tt.TenNhanVienKhongDau + ' - ' AS [text()]
    							From (select nvth.ID_ChiTietHoaDon, nv.TenNhanVienKhongDau from BH_NhanVienThucHien nvth 
    							inner join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID) tt
    							Where tt.ID_ChiTietHoaDon = hh_tt.ID_ChiTietHoaDon
    							For XML PATH ('')
    						) nhanvienchietkhau_khongdau,
							(
    							Select tt.TenNhanVienChuCaiDau + ' - ' AS [text()]
    							From (select nvth.ID_ChiTietHoaDon, nv.TenNhanVienChuCaiDau from BH_NhanVienThucHien nvth 
    							inner join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID) tt
    							Where tt.ID_ChiTietHoaDon = hh_tt.ID_ChiTietHoaDon
    							For XML PATH ('')
    						) nhanvienchietkhau_chucaidau
    						From (select nvth.ID_ChiTietHoaDon, nv.TenNhanVien from BH_NhanVienThucHien nvth 
    							inner join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID) hh_tt
    						) Main) as nv on ctsd.ID = nv.ID_ChiTietHoaDon
where  hh.LaHangHoa like @LaHangHoa
    		and hh.TheoDoi like @TheoDoi
    		and qd.Xoa like @TrangThai
			AND ((select count(Name) from @tblSearchString b 
			where hh.TenHangHoa_KhongDau like '%'+b.Name+'%'    		
    			or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa like '%'+b.Name+'%'
    			or lo.MaLoHang like '%' +b.Name +'%' 
    			or qd.MaHangHoa like '%'+b.Name+'%'
    			or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    			or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    			or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    			or qd.TenDonViTinh like '%'+b.Name+'%'
    			or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
				or dt.DienThoai like '%'+b.Name+'%'
				or dt.TenDoiTuong like '%'+b.Name+'%'
				or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				or dt.MaDoiTuong like '%'+b.Name+'%'
				or ctsd.MaHoaDon like '%'+b.Name+'%'
				or ctm.MaHoaDon like '%'+b.Name+'%'
				or ISNULL(nv.NhanVienChietKhau,'') like '%'+b.Name+'%'
				or nv.NhanVienChietKhau_ChuCaiDau like '%'+b.Name+'%'
				or nv.NhanVienChietKhau_KhongDau like '%'+b.Name+'%'
				or xe.BienSo like '%'+b.Name+'%'
				or chuxe.MaDoiTuong like '%'+b.Name+'%'
				or chuxe.TenDoiTuong like '%'+b.Name+'%'
				or chuxe.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				)=@count or @count=0)
				
) b where exists (select ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) nhom where b.ID_NhomHang = nhom.ID)
order by b.NgayLapHoaDon desc
			
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDoanhThuSuaChuaChiTiet]
    @IdChiNhanhs [nvarchar](max),
    @ThoiGianFrom [datetime],
    @ThoiGianTo [datetime],
    @DoanhThuFrom [float],
    @DoanhThuTo [float],
    @LoiNhuanFrom [float],
    @LoiNhuanTo [float],
    @TextSearch [nvarchar](max),
	@IdNhomHangHoa UNIQUEIDENTIFIER
AS
BEGIN

    SET NOCOUNT ON;
    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    
	DECLARE @tblIdNhomHangHoa TABLE(ID UNIQUEIDENTIFIER);
	IF(@IdNhomHangHoa IS NOT NULL)
	BEGIN
		WITH parents AS (
		  SELECT ID, TenNhomHangHoa, ID_Parent
		  FROM DM_NhomHangHoa
		  WHERE ID = @IdNhomHangHoa

		  UNION ALL

		  SELECT nhh.ID, nhh.TenNhomHangHoa, nhh.ID_Parent
		  FROM DM_NhomHangHoa nhh
		  INNER JOIN parents p ON nhh.ID_Parent = p.ID
		)
		INSERT INTO @tblIdNhomHangHoa SELECT ID FROM parents;
	END
	ELSE
	BEGIN
		INSERT INTO @tblIdNhomHangHoa SELECT ID FROM DM_NhomHangHoa;
	END

    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    	DECLARE @count int;
    	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    	Select @count =  (Select count(*) from @tblSearch);
    
    	DECLARE @tblHoaDonSuaChua TABLE (IDPhieuTiepNhan UNIQUEIDENTIFIER, MaPhieuTiepNhan NVARCHAR(MAX), NgayVaoXuong DATETIME, BienSo NVARCHAR(MAX), 
    	MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), CoVanDichVu NVARCHAR(MAX),
    	ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, ID_DonViQuiDoi UNIQUEIDENTIFIER, IDChiTiet UNIQUEIDENTIFIER, ID_ChiTietDinhLuong UNIQUEIDENTIFIER,
    	MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), TenDonViTinh NVARCHAR(MAX), SoLuong FLOAT, DonGia FLOAT, TienChietKhau FLOAT, ThanhTien FLOAT, TienThue FLOAT,
    	GiamGia FLOAT, DoanhThu FLOAT,
    	GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), ChiPhi FLOAT);
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT ptn.ID, ptn.MaPhieuTiepNhan, ptn.NgayVaoXuong, dmx.BienSo, dt.MaDoiTuong, dt.TenDoiTuong, nv.TenNhanVien, hd.ID,
    	hd.MaHoaDon, hd.NgayLapHoaDon, hdct.ID_DonViQuiDoi, hdct.ID, hdct.ID_ChiTietDinhLuong,
    	dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, hdct.SoLuong, 
		IIF(hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL, hdct.DonGia, 0), 
		hdct.TienChietKhau*hdct.SoLuong, 
		IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),hdct.ThanhTien, 0) AS ThanhTien, 
		IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),
		IIF(hd.TongThueKhachHang = 0 AND hd.TongTienThueBaoHiem <> 0, (hdct.DonGia - hdct.TienChietKhau)*hdct.SoLuong * (hd.TongTienThue/hd.TongTienHang), hdct.TienThue*hdct.SoLuong), 0) AS TienThue,
    	IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),
		IIF(hd.TongTienHang = 0, 0, 
		hdct.ThanhTien * hd.TongGiamGia/hd.TongTienHang),0) AS GiamGia, 
		IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),
		IIF(hd.TongTienHang =0,hdct.ThanhTien,(hdct.ThanhTien * (1 - hd.TongGiamGia/hd.TongTienHang))),0) AS DoanhThu,
    	hdct.GhiChu, dv.MaDonVi, dv.TenDonVi, 0 FROM Gara_PhieuTiepNhan ptn
    	INNER JOIN BH_HoaDon hd ON hd.ID_PhieuTiepNhan = ptn.ID
    	INNER JOIN BH_HoaDon_ChiTiet hdct ON hd.ID = hdct.ID_HoaDon
    	INNER JOIN DonViQuiDoi dvqd ON hdct.ID_DonViQuiDoi = dvqd.ID
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
		INNER JOIN @tblIdNhomHangHoa nhh ON hh.ID_NhomHang = nhh.ID
    	INNER JOIN Gara_DanhMucXe dmx ON ptn.ID_Xe = dmx.ID
    	INNER JOIN DM_DoiTuong dt ON dt.ID = ptn.ID_KhachHang
    	LEFT JOIN NS_NhanVien nv ON ptn.ID_CoVanDichVu = nv.ID
    	INNER JOIN DM_DonVi dv ON dv.ID = ptn.ID_DonVi
    	INNER JOIN @tblDonVi dvf ON dv.ID = dvf.ID_DonVi
    	WHERE hd.LoaiHoaDon = 25 AND hd.ChoThanhToan = 0  --AND ptn.TrangThai != 0 
    	AND (@ThoiGianFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @ThoiGianFrom AND @ThoiGianTo)
    	AND ((select count(Name) from @tblSearch b where     			
    			ptn.MaPhieuTiepNhan like '%'+b.Name+'%'
    			or dmx.BienSo like '%'+b.Name+'%'
    			or dt.MaDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong like '%'+b.Name+'%'
    			or nv.TenNhanVien like '%'+b.Name+'%'
    			or hd.MaHoaDon like '%'+b.Name+'%'
    			or hd.DienGiai like '%'+b.Name+'%'
				or hh.TenHangHoa like '%'+b.Name+'%'
				or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'
				or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%'
				or dvqd.MaHangHoa like '%'+b.Name+'%'
    			)=@count or @count=0);
		--SELECT * FROM @tblHoaDonSuaChua

		--SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, hdsc.IDChiTiet, hdsc.ID_ChiTietDinhLuong,
  --  	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
  --  	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
  --  	hdsc.MaHangHoa, hdsc.TenHangHoa, hdsc.TenDonViTinh, hdsc.SoLuong, hdsc.DonGia, hdsc.TienChietKhau, hdsc.ThanhTien, hdsc.TienThue,
  --  	hdsc.GiamGia, hdsc.DoanhThu,
  --  	hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
  --  	FROM @tblHoaDonSuaChua hdsc
  --  	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon
  --  	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		--AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
  --  	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL 

		DECLARE @tblGiaVonThanhPhan TABLE(IDChiTietDinhLuong UNIQUEIDENTIFIER, TongTIenVon FLOAT)
		INSERT INTO @tblGiaVonThanhPhan
		SELECT hdsc.ID_ChiTietDinhLuong, SUM(ISNULL(xkct.GiaVon,0) * ISNULL(xkct.SoLuong,0)) AS TongTienVon
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) AND hdsc.ID_ChiTietDinhLuong IS NOT NULL
		GROUP BY hdsc.ID_ChiTietDinhLuong
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(MaPhieuTiepNhan NVARCHAR(MAX), NgayVaoXuong DATETIME, BienSo NVARCHAR(MAX), IDChiTiet UNIQUEIDENTIFIER, 
    	MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), CoVanDichVu NVARCHAR(MAX),
    	ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), TenDonViTinh NVARCHAR(MAX), SoLuong FLOAT, DonGia FLOAT, TienChietKhau FLOAT, ThanhTien FLOAT, 
    	TienThue FLOAT,
    	GiamGia FLOAT, DoanhThu FLOAT, GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), TienVon FLOAT, LoiNhuan FLOAT, ChiPhi FLOAT)
    
    	INSERT INTO @tblBaoCaoDoanhThu
		SELECT bcsc.MaPhieuTiepNhan, bcsc.NgayVaoXuong, bcsc.BienSo, bcsc.IDChiTiet,
    	bcsc.MaDoiTuong, bcsc.TenDoiTuong, bcsc.CoVanDichVu,
    	bcsc.ID, bcsc.MaHoaDon, bcsc.NgayLapHoaDon,
    	bcsc.MaHangHoa, bcsc.TenHangHoa, bcsc.TenDonViTinh, bcsc.SoLuong, bcsc.DonGia, bcsc.TienChietKhau, bcsc.ThanhTien, bcsc.TienThue,
    	bcsc.GiamGia, bcsc.DoanhThu,
    	bcsc.GhiChu, bcsc.MaDonVi, bcsc.TenDonVi, SUM(ISNULL(bcsc.GiaVon,0)*ISNULL(bcsc.SoLuongxk,0)) AS TienVon,
    	bcsc.DoanhThu - SUM(ISNULL(bcsc.GiaVon,0)*ISNULL(bcsc.SoLuongxk,0)) AS LoiNhuan, 0
		FROM
    	(
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, hdsc.IDChiTiet, hdsc.ID_ChiTietDinhLuong,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
    	hdsc.MaHangHoa, hdsc.TenHangHoa, hdsc.TenDonViTinh, hdsc.SoLuong, hdsc.DonGia, hdsc.TienChietKhau, hdsc.ThanhTien, hdsc.TienThue,
    	hdsc.GiamGia, hdsc.DoanhThu,
    	hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon AND xk.ChoThanhToan = 0
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE xk.LoaiHoaDon = 8 OR xk.ID IS NULL 
		--AND (hdsc.IDChiTiet = hdsc.ID_ChiTietDinhLuong OR hdsc.ID_ChiTietDinhLuong IS NULL)
		UNION ALL
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, NULL, null,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
    	dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, 0 AS SoLuong, 0 AS DonGia, 0 AS TienChietKhau, 0 AS ThanhTien, 0 AS TienThue,
    	0 AS GiamGia, 0 AS DoanhThu,
    	'' AS GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk FROM
		(SELECT IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, 
		MaDonVi, TenDonVi, MaHoaDon, ID, NgayLapHoaDon
    	FROM @tblHoaDonSuaChua GROUP BY IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, MaDonVi, 
		TenDonVi, MaHoaDon, ID, NgayLapHoaDon)
		hdsc
    	INNER JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon and xk.ChoThanhToan = 0
		INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		INNER JOIN DonViQuiDoi dvqd ON xkct.ID_DonViQuiDoi = dvqd.ID
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
		INNER JOIN @tblIdNhomHangHoa nhh ON hh.ID_NhomHang = nhh.ID
		--AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE xk.LoaiHoaDon = 8 AND xkct.ID_ChiTietGoiDV IS NULL
		UNION ALL
		--Xuât kho cho phiếu tiếp nhận
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, NULL, null,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	null, '', null,
    	dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, 0, 0, 0, 0, 0,
    	0, 0,
    	'', hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk FROM
		(SELECT IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, MaDonVi, TenDonVi
    	FROM @tblHoaDonSuaChua GROUP BY IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, MaDonVi, TenDonVi)
		hdsc
    	INNER JOIN BH_HoaDon xk ON hdsc.IDPhieuTiepNhan = xk.ID_PhieuTiepNhan and xk.ChoThanhToan = 0
		INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		INNER JOIN DonViQuiDoi dvqd ON xkct.ID_DonViQuiDoi = dvqd.ID
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
		INNER JOIN @tblIdNhomHangHoa nhh ON hh.ID_NhomHang = nhh.ID
		--AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE xk.LoaiHoaDon = 8 AND xk.ID_HoaDon IS NULL
		) bcsc WHERE bcsc.IDChiTiet = bcsc.ID_ChiTietDinhLuong OR bcsc.ID_ChiTietDinhLuong IS NULL
		--AND ((select count(Name) from @tblSearch b where 
		--	MaHangHoa like '%'+b.Name+'%'
  --  			)=@count or @count=0) 
    	GROUP BY bcsc.MaPhieuTiepNhan, bcsc.NgayVaoXuong, bcsc.BienSo, bcsc.IDChiTiet, bcsc.ID_ChiTietDinhLuong,
    	bcsc.MaDoiTuong, bcsc.TenDoiTuong, bcsc.CoVanDichVu,
    	bcsc.ID, bcsc.MaHoaDon, bcsc.NgayLapHoaDon,
    	bcsc.MaHangHoa, bcsc.TenHangHoa, bcsc.TenDonViTinh, bcsc.SoLuong, bcsc.DonGia, bcsc.TienChietKhau, bcsc.ThanhTien, bcsc.TienThue,
    	bcsc.GiamGia, bcsc.DoanhThu,
    	bcsc.GhiChu, bcsc.MaDonVi, bcsc.TenDonVi;

		UPDATE bcdt SET
		bcdt.TienVon = gvtp.TongTIenVon, bcdt.LoiNhuan = bcdt.DoanhThu - gvtp.TongTIenVon
		FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblGiaVonThanhPhan gvtp ON bcdt.IDChiTiet = gvtp.IDChiTietDinhLuong;
    
		DECLARE @tblChiPhi TABLE(IDChiTiet UNIQUEIDENTIFIER, ChiPhi FLOAT);
		INSERT INTO @tblChiPhi
		SELECT hdcp.ID_HoaDon_ChiTiet, SUM(hdcp.ThanhTien) FROM BH_HoaDon_ChiPhi hdcp
		INNER JOIN @tblBaoCaoDoanhThu bcdt ON hdcp.ID_HoaDon_ChiTiet = bcdt.IDChiTiet
		GROUP BY hdcp.ID_HoaDon_ChiTiet

		UPDATE bcdt SET
		bcdt.ChiPhi = hdcp.ChiPhi, bcdt.LoiNhuan = bcdt.LoiNhuan - hdcp.ChiPhi
		FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblChiPhi hdcp ON bcdt.IDChiTiet = hdcp.IDChiTiet;

    	DECLARE @SThanhTien FLOAT,  @SChietKhau FLOAT, @SThue FLOAT, @SGiamGia FLOAT, @SDoanhThu FLOAT, @STongTienVon FLOAT, @SLoiNhuan FLOAT, @SChiPhi FLOAT;
    	SELECT @SThanhTien = SUM(ThanhTien), @SChietKhau = SUM(TienChietKhau), @SThue = SUM(TienThue), @SGiamGia = SUM(GiamGia), 
		@SDoanhThu = SUM(DoanhThu), @STongTienVon = SUM(TienVon), @SLoiNhuan = SUM(LoiNhuan), @SChiPhi = SUM(ChiPhi)
    	FROM @tblBaoCaoDoanhThu
    
    	SELECT IDChiTiet, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu , ID AS IDHoaDon, MaHoaDon,
    	NgayLapHoaDon, MaHangHoa, TenHangHoa, TenDonViTinh, ISNULL(SoLuong, 0) AS SoLuong, ISNULL(DonGia, 0) AS DonGia, ISNULL(TienChietKhau, 0) AS TienChietKhau, 
    	ISNULL(TienThue,0) AS TienThue, ISNULL(ThanhTien,0) AS ThanhTien, ISNULL(GiamGia, 0) AS GiamGia, ISNULL(DoanhThu, 0) AS DoanhThu, ISNULL(TienVon,0) AS TienVon, ISNULL(LoiNhuan,0) AS LoiNhuan,
    	GhiChu, MaDonVi, TenDonVi, ChiPhi, ISNULL(@SThanhTien, 0) AS SThanhTien, ISNULL(@SChietKhau,0) AS SChietKhau,
    	ISNULL(@SThue,0) AS SThue, ISNULL(@SGiamGia,0) AS SGiamGia, ISNULL(@SDoanhThu, 0) AS SDoanhThu, ISNULL(@STongTienVon,0) AS STongTienVon,
    	ISNULL(@SLoiNhuan,0) AS SLoiNhuan, ISNULL(@SChiPhi, 0) AS SChiPhi
    	FROM @tblBaoCaoDoanhThu
    	WHERE (@DoanhThuFrom IS NULL OR DoanhThu >= @DoanhThuFrom)
    	AND (@DoanhThuTo IS NULL OR DoanhThu <= @DoanhThuTo)
    	AND (@LoiNhuanFrom IS NULL OR LoiNhuan >= @LoiNhuanFrom)
    	AND (@LoiNhuanTo IS NULL OR LoiNhuan <= @LoiNhuanTo)
    	ORDER BY NgayLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKhachHang_TanSuat]
	@IDChiNhanhs nvarchar(max),	
	@IDNhomKhachs nvarchar(max),
	@LoaiChungTus varchar(20),
	@TrangThaiKhach varchar(10),
	@FromDate datetime,
	@ToDate datetime,
	@NgayGiaoDichFrom datetime,
	@NgayGiaoDichTo datetime,
	@NgayTaoKHFrom datetime,
	@NgayTaoKHTo datetime,
	@DoanhThuTu float,	
	@DoanhThuDen float,
	@SoLanDenFrom int,
	@SoLanDenTo int,
	@TextSearch nvarchar(max),
	@CurrentPage int,
	@PageSize int,
	@ColumnSort varchar(200),
	@TypeSort varchar(20)
AS
BEGIN
	
	SET NOCOUNT ON;
	set @ToDate = DATEADD(day, 1, @ToDate)

	if @ColumnSort ='' or @ColumnSort is null set @ColumnSort='MaKhachHang'
	if @TypeSort ='' or @TypeSort is null set @TypeSort='desc'
	SET @TypeSort = UPPER(@TypeSort)
	 

	 DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	declare @tblChiNhanh table (ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh
	select * from dbo.splitstring(@IDChiNhanhs)
	
	declare @tblNhomDT table (ID varchar(40))
	insert into @tblNhomDT
	select Name from dbo.splitstring(@IDNhomKhachs);

	declare @tblLoaiHD table (Loai varchar(3))
	insert into @tblLoaiHD
	select Name from dbo.splitstring(@LoaiChungTus);

	
	if @DoanhThuTu is null
		set @DoanhThuTu = -10000000

	if @DoanhThuDen is null
		set @DoanhThuDen = 9999999999
	if @SoLanDenTo is null
		set @SoLanDenTo = 9999999;

	with data_cte
	as(
	select dt.ID as ID_KhachHang, dt.MaDoiTuong as MaKhachHang, dt.TenDoiTuong as TenKhachHang, dt.DienThoai, dt.DiaChi, dt.TenNhomDoiTuongs,
		dt.NgayGiaoDichGanNhat,
		hd1.SoLanDen,
		GiaTriMua, 
		GiaTriTra,
		DoanhThu
	from DM_DoiTuong dt  
	join (		
			select hd.ID_DoiTuong, 
			count(hd.ID) as SoLanDen, 					
			sum(isnull(hd.GiaTriTra,0)) as GiaTriTra,
			sum(hd.GiaTriMua) as GiaTriMua,
			sum(hd.GiaTriMua - hd.GiaTriTra) as DoanhThu
			from(
				-- doanhthu: khong tinh napthe (chi tinh luc su dung)
				-- vi BC chi tiet theo khachhang: khong lay duoc dichvu/hanghoa khi napthe
				select  hd.ID_DoiTuong, hd.ID,
					iif(hd.LoaiHoaDon= 6, hd.TongTienHang - hd.TongGiamGia,0) as GiaTriTra,
					iif(hd.LoaiHoaDon!= 6, hd.TongTienHang - hd.TongGiamGia - isnull(hd.KhuyeMai_GiamGia,0),0) as GiaTriMua					
				from BH_HoaDon hd
				where hd.ChoThanhToan = 0
				and hd.LoaiHoaDon != 22
				and hd.ID_DoiTuong is not null
				and hd.NgayLapHoaDon between @FromDate and  @ToDate
				and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
				and exists (select Loai from @tblLoaiHD loai where hd.LoaiHoaDon= loai.Loai)
			) hd group by hd.ID_DoiTuong			
	) hd1 on dt.ID = hd1.ID_DoiTuong
    where 
		 exists (select nhom1.Name 
			from dbo.splitstring(iif(dt.IDNhomDoiTuongs is null or dt.IDNhomDoiTuongs ='','00000000-0000-0000-0000-000000000000',dt.IDNhomDoiTuongs)) nhom1 
		 join @tblNhomDT nhom2 on nhom1.Name = nhom2.ID) 
	and
		dt.TheoDoi like @TrangThaiKhach
	and dt.NgayGiaoDichGanNhat between @NgayGiaoDichFrom and  @NgayGiaoDichTo
	and ((dt.NgayTao between @NgayTaoKHFrom and @NgayTaoKHTo) or ( dt.ID ='00000000-0000-0000-0000-000000000000'))
	and hd1.SoLanDen between @SoLanDenFrom and @SoLanDenTo
	and hd1.DoanhThu between @DoanhThuTu and @DoanhThuDen
	and ((select count(Name) from @tblSearchString b where 
    				dt.MaDoiTuong like '%'+b.Name+'%' 
    				or dt.TenDoiTuong like '%'+b.Name+'%' 
    				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    				or dt.TenDoiTuong_ChuCaiDau like '%' +b.Name +'%' 
    				or dt.DienThoai like '%'+b.Name+'%'
					or dt.DiaChi like '%'+b.Name+'%'
					)=@count or @count=0)
	
	),
	count_cte
	as(
	select count(ID_KhachHang) as TotalRow,
		CEILING(COUNT(ID_KhachHang) / CAST(@PageSize as float )) as TotalPage,
		sum(dt.SoLanDen) as TongSoLanDen,
		sum(GiaTriMua) as TongMua,
		sum(GiaTriTra) as TongTra,
		sum(DoanhThu) as TongDoanhThu
	from data_cte dt
	)
	select *
	from data_cte dt
	cross join count_cte cte
	order by	
		-- các cột dữ liệu sắp xếp phải chuyển về cùng 1 kiểu data
		CASE WHEN @TypeSort = 'ASC'  THEN 
			case @ColumnSort		
				when 'SoLanDen' then cast(dt.SoLanDen as float)
				when 'DoanhThu' then cast(dt.DoanhThu as float)
				when 'GiaTriMua' then cast(dt.GiaTriMua as float)
				when 'GiaTriTra' then cast(dt.GiaTriTra as float)
				when 'NgayGiaoDichGanNhat' then cast(dt.NgayGiaoDichGanNhat as float)
				end
			 END ASC,
		CASE WHEN @TypeSort = 'DESC'  THEN 
			case @ColumnSort
				when 'SoLanDen' then cast(dt.SoLanDen as float)
				when 'DoanhThu' then cast(dt.DoanhThu as float)
				when 'GiaTriMua' then cast(dt.GiaTriMua as float)
				when 'GiaTriTra' then cast(dt.GiaTriTra as float)
				when 'NgayGiaoDichGanNhat' then cast(dt.NgayGiaoDichGanNhat as float)
			end
		END DESC
	OFFSET (@CurrentPage* @PageSize) ROWS
	FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_ChiTietHangNhap]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
	@timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@LoaiChungTu [nvarchar](max)
AS
BEGIN

    SET NOCOUNT ON;
	
	DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER);
	INSERT INTO @tblIdDonVi
	SELECT donviinput.Name FROM [dbo].[splitstring](@ID_DonVi) donviinput

	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung)
    
    DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	DECLARE @tblHoaDon TABLE(MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, DienGiai NVARCHAR(max), TenNhomHang NVARCHAR(MAX), 
	MaHangHoa NVARCHAR(MAX), TenHangHoaFull NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), ThuocTinh_GiaTri NVARCHAR(MAX),
	TenDonViTinh NVARCHAR(MAX), TenLoHang NVARCHAR(MAX),
	ID_DonVi UNIQUEIDENTIFIER,
	LoaiHoaDon INT, TyLeChuyenDoi FLOAT,
	SoLuong FLOAT, TienChietKhau FLOAT, ThanhTien FLOAT, GiaVon FLOAT, YeuCau NVARCHAR(MAX), GiamGiaHDPT FLOAT, GhiChu nvarchar(max));
	INSERT INTO @tblHoaDon

	select 
		MaHoaDon,
		NgayLapHoaDon,
		DienGiai,
		TenNhomHang,
		MaHangHoa,
		TenHangHoaFull,
		TenHangHoa,
		ThuocTinh_GiaTri, 
		TenDonViTinh,
		TenLoHang,
		ID_DonVi,
		LoaiHoaDon,
		TyLeChuyenDoi,
		SoLuong,
		TienChietKhau,
		ThanhTien,
		GiaVon,
		YeuCau,
		GianGiaHD,
		GhiChu
	from
	(
	SELECT bhd.MaHoaDon, IIF(bhd.LoaiHoaDon = 10 and bhd.YeuCau = '4', bhd.NgaySua, bhd.NgayLapHoaDon) AS NgayLapHoaDon,
		bhd.DienGiai,
		nhh.TenNhomHangHoa AS TenNhomHang, 
		nhh.TenNhomHangHoa_KhongDau AS TenNhomHangHoa_KhongDau, 
		dvqdChuan.MaHangHoa, 
		concat(hh.TenHangHoa , dvqd.ThuocTinhGiaTri) AS TenHangHoaFull,
		hh.TenHangHoa, 
		ISNULL(dvqd.ThuocTinhGiaTri, '') AS ThuocTinh_GiaTri, 
		dvqdChuan.TenDonViTinh,
		lh.MaLoHang AS TenLoHang, 
		iif(bhd.LoaiHoaDon=10, bhd.ID_CheckIn,bhd.ID_DonVi) as ID_DonVi,	
		bhd.LoaiHoaDon, 
		dvqd.TyLeChuyenDoi, 
		bhdct.SoLuong,
		bhdct.TienChietKhau,
		bhdct.ThanhTien, 
		case bhd.LoaiHoaDon
			when 6 then case when ctm.GiaVon is null or bhdct.ID_ChiTietDinhLuong is null then bhdct.GiaVon else ctm.GiaVon end
			when 10 then bhdct.DonGia ---- chuyenhang: lay giatri chuyen (vi giavon luon thay doi)
		else bhdct.GiaVon end as GiaVon,		
		bhd.YeuCau,
		IIF(bhd.TongTienHang = 0, 0, bhd.TongGiamGia / bhd.TongTienHang) as GianGiaHD,
		bhdct.GhiChu,
		hh.TenHangHoa_KhongDau,
		hh.TenHangHoa_KyTuDau,
		iif(@SearchString='',bhdct.GhiChu, dbo.FUNC_ConvertStringToUnsign(bhdct.GhiChu)) as GhiChuUnsign,
		iif(@SearchString='',bhd.DienGiai, dbo.FUNC_ConvertStringToUnsign(bhd.DienGiai)) as DienGiaiUnsign
    FROM BH_HoaDon_ChiTiet bhdct
	left join BH_HoaDon_ChiTiet ctm on bhdct.ID_ChiTietGoiDV = ctm.ID
    INNER JOIN BH_HoaDon bhd ON bhdct.ID_HoaDon = bhd.ID
	join @tblIdDonVi dv on (bhd.ID_DonVi = dv.ID and bhd.LoaiHoaDon!=10) or (bhd.ID_CheckIn = dv.ID and bhd.YeuCau='4')
    INNER JOIN DonViQuiDoi dvqd ON dvqd.ID = bhdct.ID_DonViQuiDoi
	INNER JOIN DonViQuiDoi dvqdChuan ON dvqdChuan.ID_HangHoa = dvqd.ID_HangHoa AND dvqdChuan.LaDonViChuan = 1
	INNER JOIN (select Name from splitstring(@LoaiChungTu)) lhd ON bhd.LoaiHoaDon = lhd.Name
	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
	INNER JOIN DM_NhomHangHoa nhh  ON nhh.ID = hh.ID_NhomHang   
    INNER JOIN (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)) allnhh  ON nhh.ID = allnhh.ID   
	LEFT JOIN DM_LoHang lh   ON lh.ID = bhdct.ID_LoHang OR (bhdct.ID_LoHang IS NULL AND lh.ID IS NULL)
    WHERE bhd.ChoThanhToan = 0
	and (bhdct.ChatLieu is null or bhdct.ChatLieu !='2')
    AND IIF(bhd.LoaiHoaDon = 10 and bhd.YeuCau = '4', bhd.NgaySua, bhd.NgayLapHoaDon) >= @timeStart
	AND IIF(bhd.LoaiHoaDon = 10 and bhd.YeuCau = '4', bhd.NgaySua, bhd.NgayLapHoaDon) < @timeEnd
	AND hh.LaHangHoa = 1 AND hh.TheoDoi LIKE @TheoDoi AND dvqd.Xoa LIKE @TrangThai 
	) tbl 
	where ((select count(Name) from @tblSearchString b where 
    		tbl.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    	
    			or tbl.TenHangHoa like N'%'+b.Name+'%'
    			or tbl.TenLoHang like N'%' +b.Name +'%' 
    			or tbl.MaHangHoa like N'%'+b.Name+'%'
				or tbl.MaHangHoa like N'%'+b.Name+'%'
    			or tbl.TenNhomHang like N'%'+b.Name+'%'
    			or tbl.TenNhomHangHoa_KhongDau like N'%'+b.Name+'%'
    			or tbl.TenDonViTinh like N'%'+b.Name+'%'
    			or tbl.ThuocTinh_GiaTri like '%'+b.Name+'%'
				or tbl.DienGiai like N'%'+b.Name+'%'
				or tbl.MaHoaDon like N'%'+b.Name+'%'
    			or tbl.GhiChu like N'%'+b.Name+'%'
				or tbl.GhiChuUnsign like '%'+b.Name+'%'
				or tbl.DienGiaiUnsign like '%'+b.Name+'%'
				)=@count or @count=0);	

	SELECT ct.TenLoaiChungTu as LoaiHoaDon, pstk.MaHoaDon, pstk.NgayLapHoaDon, pstk.TenNhomHang, pstk.MaHangHoa, pstk.TenHangHoaFull,
	pstk.TenHangHoa, pstk.ThuocTinh_GiaTri, pstk.TenDonViTinh, pstk.TenLoHang,DienGiai, pstk.GhiChu,
	dv.TenDonVi, dv.MaDonVi,  pstk.SoLuongNhap AS SoLuong, IIF(@XemGiaVon = '1',ROUND(pstk.GiaTriNhap,0) , 0) as ThanhTien
	FROM 
	(
    SELECT 
    MaHoaDon, NgayLapHoaDon, TenNhomHang, MaHangHoa, TenHangHoaFull, TenHangHoa, ThuocTinh_GiaTri, TenDonViTinh, TenLoHang, ID_DonVi,
	LoaiHoaDon, DienGiai, GhiChu,
	IIF(LoaiHoaDon = 10 and YeuCau = '4', TienChietKhau* TyLeChuyenDoi, SoLuong * TyLeChuyenDoi) AS SoLuongNhap,
	IIF(LoaiHoaDon = 10 and YeuCau = '4' ,TienChietKhau* GiaVon, iif(LoaiHoaDon = 6, SoLuong * GiaVon,   ThanhTien*(1-GiamGiaHDPT))) AS GiaTriNhap
    FROM @tblHoaDon WHERE LoaiHoaDon != 9

	UNION ALL
    SELECT 
    MaHoaDon, NgayLapHoaDon, TenNhomHang, MaHangHoa, TenHangHoaFull, TenHangHoa, ThuocTinh_GiaTri, TenDonViTinh, TenLoHang, ID_DonVi,
	LoaiHoaDon,DienGiai, GhiChu,
	sum(SoLuong * TyLeChuyenDoi) as SoLuongNhap,	
	SUM(SoLuong * TyLeChuyenDoi * GiaVon) AS GiaTriNhap
    FROM @tblHoaDon
    WHERE LoaiHoaDon = 9 and SoLuong > 0 
    GROUP BY LoaiHoaDon, MaHoaDon, NgayLapHoaDon,TenNhomHang, MaHangHoa, TenHangHoaFull, TenHangHoa, ThuocTinh_GiaTri, TenDonViTinh, TenLoHang, ID_DonVi,DienGiai, GhiChu
	) pstk
	join DM_DonVi dv on pstk.ID_DonVi= dv.ID
	INNER JOIN DM_LoaiChungTu ct on pstk.LoaiHoaDon = ct.ID
	order by pstk.NgayLapHoaDon desc

END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_ChiTietHangXuat]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
	@timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@LoaiChungTu [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
	 DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER);
    INSERT INTO @tblChiNhanh SELECT Name FROM splitstring(@ID_DonVi)

	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung)
    
    DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
	
			select 
				dv.MaDonVi, dv.TenDonVi, nv.TenNhanVien,
				tblQD.NgayLapHoaDon, tblQD.MaHoaDon,
				tblQD.BienSo,
				isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
				isnull(lo.MaLoHang,'') as TenLoHang,
				qd.MaHangHoa, qd.TenDonViTinh, 
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				hh.TenHangHoa,
				CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,
				tblQD.GhiChu,
				tblQD.DienGiai,
				round(SoLuong,3) as SoLuong,
				iif(@XemGiaVon='1', round(GiaTriXuat,3),0) as ThanhTien,
				case tblQD.LoaiXuatKho
					when 1 then N'Hóa đơn bán lẻ'
					when 2 then N'Xuất sử dụng gói dịch vụ'
					when 3 then N'Xuất bán dịch vụ định lượng'
					when 11 then N'Xuất sửa chữa'
					when 7 then N'Trả hàng nhà cung cấp'
					when 8 then N'Xuất kho'
					when 9 then N'Phiếu kiểm kê'
					when 10 then N'Chuyển hàng'				
				end as LoaiHoaDon
			from
			(
					select 
						qd.ID_HangHoa,
						tblHD.ID_LoHang, 
						tblHD.ID_DonVi,
						tblHD.ID_CheckIn,
						tblHD.NgayLapHoaDon,
						tblHD.MaHoaDon, 
						tblHD.ID_NhanVien,
						tblHD.LoaiHoaDon,
						tblHD.LoaiXuatKho,
						tblHD.BienSo,
						tblHD.DienGiai,
						iif(@SearchString='',tblHD.DienGiai,dbo.FUNC_ConvertStringToUnsign(tblHD.DienGiai)) as DienGiaiUnsign,
						max(tblHD.GhiChu) as GhiChu,
						iif(@SearchString='',max(tblHD.GhiChu),max(dbo.FUNC_ConvertStringToUnsign(tblHD.GhiChu))) as GhiChuUnsign,
						sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
						sum(tblHD.GiaTriXuat) as GiaTriXuat
					from
					(
						select 
							hd.NgayLapHoaDon, 
							hd.MaHoaDon,
							hd.LoaiHoaDon,
							hd.ID_HoaDon,
							hd.ID_CheckIn,
							hd.ID_NhanVien, 
							hd.DienGiai, 
							xe.BienSo,
							case hd.LoaiHoaDon
								when 8 then case when hd.ID_PhieuTiepNhan is not null then case when ct.ChatLieu= 4 then 2 else  11 end -- xuat suachua
									else 8 end
								when 1 then case when hd.ID_HoaDon is null and ct.ID_ChiTietGoiDV is not null then 2 --- xuat sudung gdv
									else case when ct.ID_ChiTietDinhLuong is not null then 3 --- xuat ban dinhluong
									else 1 end end -- xuat banle
								else hd.LoaiHoaDon end as LoaiXuatKho, -- xuat khac: traNCC, chuyenang,..
							ct.ID_ChiTietGoiDV,
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							case hd.LoaiHoaDon
							when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0)
							when 10 then ct.TienChietKhau else ct.SoLuong end as SoLuong,
							ct.TienChietKhau,
							ct.GiaVon,
							ct.GhiChu,
							case hd.LoaiHoaDon
								when 7 then ct.SoLuong * ct.DonGia
								when 10 then ct.TienChietKhau * ct.DonGia --- chuyenhang: always get DonGia
								when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0) * ct.GiaVon 
								else ct.SoLuong* ct.GiaVon end as GiaTriXuat
						from BH_HoaDon_ChiTiet ct
						join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
						left join Gara_PhieuTiepNhan tn on hd.ID_PhieuTiepNhan = tn.ID
						left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
						WHERE hd.ChoThanhToan = 0 
						AND hd.NgayLapHoaDon between @timeStart AND @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						and hd.LoaiHoaDon not in (3,4,6,19,25)
						and iif(hd.LoaiHoaDon=9,ct.SoLuong, -1) < 0 -- phieukiemke: chi lay neu soluong < 0 (~xuatkho)
					) tblHD
				join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID			
				where exists (select Name from dbo.splitstring(@LoaiChungTu) loaict where tblHD.LoaiXuatKho = loaict.Name)				
				group by qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi,
				 tblHD.ID_CheckIn,tblHD.NgayLapHoaDon, tblHD.MaHoaDon, tblHD.ID_NhanVien, 
				 tblHD.LoaiHoaDon, tblHD.LoaiXuatKho, tblHD.DienGiai, tblHD.BienSo			
			)tblQD
			join DM_DonVi dv on tblQD.ID_DonVi = dv.ID
			join DM_LoaiChungTu ct on tblQD.LoaiHoaDon = ct.ID
			left join NS_NhanVien nv on tblQD.ID_NhanVien= nv.ID
			join DM_HangHoa hh on tblQD.ID_HangHoa= hh.ID
			join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
			left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
			left join DM_LoHang lo on tblQD.ID_LoHang= lo.ID and (lo.ID= tblQD.ID_LoHang or (tblQD.ID_LoHang is null and lo.ID is null))
			where hh.LaHangHoa = 1
			and hh.TheoDoi like @TheoDoi
			and qd.Xoa like @TrangThai
			and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID= allnhh.ID)
			AND ((select count(Name) from @tblSearchString b where 
    		hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa like '%'+b.Name+'%'
    		or lo.MaLoHang like '%' +b.Name +'%' 
			or qd.MaHangHoa like '%'+b.Name+'%'
			or qd.MaHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    		or qd.TenDonViTinh like '%'+b.Name+'%'
    		or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
			or dv.MaDonVi like '%'+b.Name+'%'
			or dv.TenDonVi like '%'+b.Name+'%'
			or nv.TenNhanVien like '%'+b.Name+'%'
			or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
			or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
			or nv.MaNhanVien like '%'+b.Name+'%'
			or tblQD.GhiChu like N'%'+b.Name+'%'
			or tblQD.MaHoaDon like N'%'+b.Name+'%'
			or tblQD.BienSo like N'%'+b.Name+'%'
			or tblQD.GhiChuUnsign like '%'+b.Name+'%'
			or tblQD.DienGiai like N'%'+b.Name+'%'
			or tblQD.DienGiaiUnsign like N'%'+b.Name+'%'
			)=@count or @count=0)
			order by tblQD.NgayLapHoaDon desc


END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_NhapChuyenHang]
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang UNIQUEIDENTIFIER,
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	 SET NOCOUNT ON;
	 DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER);
	 INSERT INTO @tblIdDonVi
	 SELECT Name FROM [dbo].[splitstring](@ID_ChiNhanh) 

	 declare @tblNhomHang table(ID UNIQUEIDENTIFIER)
	insert into @tblNhomHang
	SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From HT_NguoiDung nd	    		
    		where nd.ID = @ID_NguoiDung);

	
	-- because like @TheoDoi was slow (không hiểu tại sao chỉ BC này bị chậm vi like @TheoDoi, các BC khác vẫn bình thường)
	declare @sTrangThai varchar(10) ='0,1'
	  set @TheoDoi= REPLACE(@TheoDoi,'%','')
		if @TheoDoi !=''
		set @sTrangThai= @TheoDoi	

    select 
				dv.MaDonVi, dv.TenDonVi,
				isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
				isnull(lo.MaLoHang,'') as TenLoHang,
				qd.MaHangHoa, qd.TenDonViTinh, 
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				hh.TenHangHoa,
				CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,
				round(SoLuong,3) as SoLuong,
				iif(@XemGiaVon='1', ThanhTien,0) as ThanhTien
			from
			(
				select 
					qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_CheckIn,
					sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
					sum(ThanhTien) as ThanhTien
				from(
				select ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_CheckIn,
					sum(ct.TienChietKhau) as SoLuong,
					sum(ct.TienChietKhau * ct.GiaVon) as ThanhTien
				from BH_HoaDon_ChiTiet ct
				join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
				where hd.ChoThanhToan=0
				and hd.LoaiHoaDon= 10 and (hd.YeuCau='1' or hd.YeuCau='4') --- YeuCau: 1.DangChuyen, 4.DaNhan, 2.PhieuTam, 3.Huy
				and hd.NgaySua between @timeStart and @timeEnd
				and exists (select ID from @tblIdDonVi dv where hd.ID_CheckIn= dv.ID)
				group by ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_CheckIn
				)tblHD
				join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID
				group by qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_CheckIn
			)tblQD
			join DM_DonVi dv on tblQD.ID_CheckIn = dv.ID
			join DM_HangHoa hh on tblQD.ID_HangHoa= hh.ID
			join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
			left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID			
			left join DM_LoHang lo on tblQD.ID_LoHang= lo.ID and (lo.ID= tblQD.ID_LoHang or tblQD.ID_LoHang is null and lo.ID is null)
			where hh.LaHangHoa = 1
			and exists (select Name from dbo.splitstring(@sTrangThai) tt where hh.TheoDoi= tt.Name )
			and qd.Xoa like @TrangThai
			and exists (SELECT ID FROM @tblNhomHang allnhh where nhom.ID= allnhh.ID)
			AND ((select count(Name) from @tblSearchString b where 
    		hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa like '%'+b.Name+'%'
    		or lo.MaLoHang like '%' +b.Name +'%' 
			or qd.MaHangHoa like '%'+b.Name+'%'
			or qd.MaHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    		or qd.TenDonViTinh like '%'+b.Name+'%'
    		or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
			or dv.MaDonVi like '%'+b.Name+'%'
			or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
		order by dv.TenDonVi, hh.TenHangHoa, lo.MaLoHang	
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_NhapXuatTon]
	@ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
    @timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
    @CoPhatSinh [int]
AS
BEGIN

	SET NOCOUNT ON;

    DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER, MaDonVi nvarchar(max), TenDonVi nvarchar(max));
    INSERT INTO @tblChiNhanh 
	SELECT dv.ID, dv.MaDonVi, dv.TenDonVi 
	FROM splitstring(@ID_DonVi) cn
	join DM_DonVi  dv on cn.Name= dv.ID

	declare @dtNow datetime = format(getdate(),'yyyy-MM-dd')  

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung);
	

	declare @tkDauKy table (ID_DonVi uniqueidentifier,ID_HangHoa uniqueidentifier,	ID_LoHang uniqueidentifier null, TonKho float,GiaVon float)		
	insert into @tkDauKy
	exec dbo.GetAll_TonKhoDauKy @ID_DonVi, @timeStart

	

			------ tonkho trongky
			select 			
				qd.ID_HangHoa,
				tkNhapXuat.ID_LoHang,
				tkNhapXuat.ID_DonVi,				
				sum(tkNhapXuat.SoLuongNhap * qd.TyLeChuyenDoi) as SoLuongNhap,
				sum(tkNhapXuat.GiaTriNhap ) as GiaTriNhap,
				sum(tkNhapXuat.SoLuongXuat * qd.TyLeChuyenDoi) as SoLuongXuat,
				sum(tkNhapXuat.GiaTriXuat) as GiaTriXuat
				into #temp
			from
			(
			-- xuat ban, trahang ncc, xuatkho, xuat chuyenhang
				select 
					ct.ID_DonViQuiDoi,
					ct.ID_LoHang,
					hd.ID_DonVi,
					0 AS SoLuongNhap,
					0 AS GiaTriNhap,
					sum(
						case hd.LoaiHoaDon
						when 10 then ct.TienChietKhau
						else ct.SoLuong end ) as SoLuongXuat,
					sum( 
						case hd.LoaiHoaDon
						when 7 then ct.SoLuong* ct.DonGia
						when 10 then ct.TienChietKhau * ct.GiaVon
						else ct.SoLuong* ct.GiaVon end )  AS GiaTriXuat
					FROM BH_HoaDon_ChiTiet ct
				LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
				WHERE hd.ChoThanhToan = 0
				and (hd.LoaiHoaDon in (1,5,7,8) 
					or (hd.LoaiHoaDon = 10  and (hd.YeuCau='1' or hd.YeuCau='4')) )
				AND hd.NgayLapHoaDon between  @timeStart AND   @timeEnd
				and hd.ID_DonVi in (select ID from @tblChiNhanh)
				GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi								


				UNION ALL
				 ---nhap chuyenhang
				SELECT 
					ct.ID_DonViQuiDoi,
					ct.ID_LoHang,
					hd.ID_CheckIn AS ID_DonVi,
					SUM(ct.TienChietKhau) AS SoLuongNhap,
					SUM(ct.TienChietKhau* ct.DonGia) AS GiaTriNhap, -- lay giatri tu chinhanh chuyen
					0 AS SoLuongXuat,
					0 AS GiaTriXuat
				FROM BH_HoaDon_ChiTiet ct
				LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
				WHERE hd.LoaiHoaDon = 10 and hd.YeuCau = '4' AND hd.ChoThanhToan = 0
				and hd.ID_CheckIn in (select ID from @tblChiNhanh)
				AND hd.NgaySua between  @timeStart AND   @timeEnd
				GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_CheckIn

    			UNION ALL
				 ---nhaphang + khach trahang
				SELECT 
					ct.ID_DonViQuiDoi,
					ct.ID_LoHang,
					hd.ID_DonVi,
					SUM(ct.SoLuong) AS SoLuongNhap,
					--- KH trahang: giatrinhap = giavon (khong lay giaban)
					sum(case hd.LoaiHoaDon
						when 6 then iif(ctm.GiaVon is null or ctm.ID = ctm.ID_ChiTietDinhLuong, ct.SoLuong * ct.GiaVon, ct.SoLuong *ctm.GiaVon)
						when 4 then iif( hd.TongTienHang = 0,0, ct.SoLuong* (ct.DonGia - ct.TienChietKhau) * (1- hd.TongGiamGia/hd.TongTienHang))
					else ct.SoLuong * ct.GiaVon end) as GiaTriNhap,
					0 AS SoLuongXuat,
					0 AS GiaTriXuat
				FROM BH_HoaDon_ChiTiet ct
				LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
				left join BH_HoaDon_ChiTiet ctm on ct.ID_ChiTietGoiDV = ctm.ID
				WHERE hd.LoaiHoaDon in (4,6,13,14)
				AND hd.ChoThanhToan = 0
				and (ct.ChatLieu is null or ct.ChatLieu !='2')
				and hd.ID_DonVi in (select ID from @tblChiNhanh)
				AND hd.NgayLapHoaDon between  @timeStart AND   @timeEnd
				GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi
    
    			UNION ALL
				-- kiemke
    			SELECT 
					ctkk.ID_DonViQuiDoi, 
					ctkk.ID_LoHang, 
					ctkk.ID_DonVi, 
					sum(isnull(SoLuongNhap,0)) as SoLuongNhap,
					sum(isnull(SoLuongNhap,0) * ctkk.GiaVon) as GiaTriNhap,
					sum(isnull(SoLuongXuat,0)) as SoLuongXuat,
					sum(isnull(SoLuongXuat,0) * ctkk.GiaVon) as GiaTriXuat
				FROM
    			(SELECT 
    				ct.ID_DonViQuiDoi,
    				ct.ID_LoHang,
					hd.ID_DonVi,
					IIF(ct.SoLuong< 0, 0, ct.SoLuong) as SoLuongNhap,
					IIF(ct.SoLuong < 0, - ct.SoLuong, 0) as SoLuongXuat,
					ct.GiaVon
    			FROM BH_HoaDon_ChiTiet ct 
    			LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
    			WHERE hd.LoaiHoaDon = '9' 
    			AND hd.ChoThanhToan = 0
				and hd.ID_DonVi in (select ID from @tblChiNhanh)
    			AND hd.NgayLapHoaDon between  @timeStart AND   @timeEnd  			
				) ctkk	
    			GROUP BY ctkk.ID_DonViQuiDoi, ctkk.ID_LoHang, ctkk.ID_DonVi
			)tkNhapXuat
			join DonViQuiDoi qd on tkNhapXuat.ID_DonViQuiDoi = qd.ID
			group by qd.ID_HangHoa, tkNhapXuat.ID_LoHang, tkNhapXuat.ID_DonVi


			if	@CoPhatSinh= 2
					begin
							select 
							a.TenNhomHang,
							a.TenHangHoa,
							a.MaHangHoa,
							a.TenDonViTinh,
							a.TenLoHang,
							a.TenDonVi,
							a.MaDonVi,
							concat(a.TenHangHoa,a.ThuocTinhGiaTri) as TenHangHoaFull,
							-- dauky
							isnull(a.TonDauKy,0) as TonDauKy,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0),0) as GiaTriDauKy,

							--- trongky
							isnull(a.SoLuongNhap,0) as SoLuongNhap,
							iif(@XemGiaVon='1',isnull(a.GiaTriNhap,0),0) as GiaTriNhap,
							isnull(a.SoLuongXuat,0) as SoLuongXuat,
							iif(@XemGiaVon='1',isnull(a.GiaTriXuat,0),0) as GiaTriXuat,

							-- cuoiky
							isnull(a.TonDauKy,0) + isnull(a.SoLuongNhap,0) - isnull(a.SoLuongXuat,0) as TonCuoiKy,
							(isnull(a.TonDauKy,0) + isnull(a.SoLuongNhap,0) - isnull(a.SoLuongXuat,0)) * iif(a.QuyCach=0 or a.QuyCach is null,1, a.QuyCach)  as TonQuyCach,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0) + isnull(a.GiaTriNhap,0) - isnull(a.GiaTriXuat,0),0)  as GiaTriCuoiKy
						from
						(
							select 
								isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
								hh.TenHangHoa,
								hh.QuyCach,
								qd.MaHangHoa,
								qd.TenDonViTinh,
								isnull(lo.MaLoHang,'') as TenLoHang,
								dv.TenDonVi,
								dv.MaDonVi,
								qd.ThuocTinhGiaTri,
				
								-- dauky	

								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) as TonDauKy,		
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) * iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaTriDauKy,
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaVon,	
							
									----trongky
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap, 0) as SoLuongNhap,		
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat, 0) as SoLuongXuat,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.GiaTriNhap, 0) as GiaTriNhap,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.GiaTriXuat, 0) as GiaTriXuat
								
							from #temp tkTrongKy
							left join DM_HangHoa hh on tkTrongKy.ID_HangHoa= hh.ID
							left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
							left join DonViQuiDoi qd on tkTrongKy.ID_HangHoa = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
							left join DM_LoHang lo on tkTrongKy.ID_LoHang= lo.ID or lo.ID is null
							cross join @tblChiNhanh  dv									
							left join @tkDauKy tkDauKy on hh.ID = tkDauKy.ID_HangHoa 
							and tkTrongKy.ID_DonVi= tkDauKy.ID_DonVi and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null and hh.QuanLyTheoLoHang = 0 ))							
							where hh.LaHangHoa= 1
								AND hh.TheoDoi LIKE @TheoDoi	
								and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
								and dv.ID in (select ID from @tblChiNhanh)
									AND ((select count(Name) from @tblSearchString b where 
    								hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    								or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    									or hh.TenHangHoa like '%'+b.Name+'%'
    									or lo.MaLoHang like '%' +b.Name +'%' 
    									or qd.MaHangHoa like '%'+b.Name+'%'
    									or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    									or qd.TenDonViTinh like '%'+b.Name+'%'
    									or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
										or dv.MaDonVi like '%'+b.Name+'%'
										or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)		
							) a	order by TenHangHoa, TenDonVi,TenLoHang		
					end
			else
			begin
			
					select 	
							dv.MaDonVi, dv.TenDonVi,
							qd.MaHangHoa,
							hh.TenHangHoa,
							hh.QuyCach,
							lo.ID as ID_LoHang,
							qd.ID as ID_DonViQuyDoi,
							concat(hh.TenHangHoa,ThuocTinhGiaTri) as TenHangHoaFull,
							isnull(ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
							isnull(qd.TenDonViTinh,'') as TenDonViTinh,
							isnull(lo.MaLoHang,'') as TenLoHang,

							---- dauky
							isnull(tkDauKy.TonKho,0) as TonDauKy,
							isnull(tkDauKy.GiaVon,0) as GiaVon,				
							iif(@XemGiaVon='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0),0)  as GiaTriDauKy,			
							isnull(nhom.TenNhomHangHoa,N'Nhóm Hàng Hóa Mặc Định') TenNhomHang,

							---- trongky
							isnull(tkTrongKy.SoLuongNhap,0) as SoLuongNhap,
							isnull(tkTrongKy.SoLuongXuat,0) as SoLuongXuat,
							iif(@XemGiaVon='1',isnull(tkTrongKy.GiaTriNhap,0),0) as GiaTriNhap,
							iif(@XemGiaVon='1',isnull(tkTrongKy.GiaTriXuat,0),0) as GiaTriXuat,

							---- cuoiky
							isnull(tkDauKy.TonKho,0) + isnull(tkTrongKy.SoLuongNhap,0) - isnull(tkTrongKy.SoLuongXuat,0) as TonCuoiKy,
							(isnull(tkDauKy.TonKho,0) + isnull(tkTrongKy.SoLuongNhap,0) - isnull(tkTrongKy.SoLuongXuat,0))  * iif(hh.QuyCach=0 or hh.QuyCach is null,1, hh.QuyCach) as TonQuyCach,
							iif(@XemGiaVon='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0) + isnull(tkTrongKy.GiaTriNhap,0)
							- isnull(tkTrongKy.GiaTriXuat,0),0) as GiaTriCuoiKy
					from DM_HangHoa hh 		
					join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
					left join DM_LoHang lo on hh.ID = lo.ID_HangHoa
					left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
					cross join @tblChiNhanh  dv		
					left join @tkDauKy tkDauKy 
					on qd.ID_HangHoa = tkDauKy.ID_HangHoa and tkDauKy.ID_DonVi= dv.ID and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null ))
					left join #temp tkTrongKy on qd.ID_HangHoa = tkTrongKy.ID_HangHoa and tkTrongKy.ID_DonVi= dv.ID and ((lo.ID= tkTrongKy.ID_LoHang) or (lo.ID is null ))
					where hh.LaHangHoa= 1
					AND hh.TheoDoi LIKE @TheoDoi	
					and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
					and dv.ID in (select ID from @tblChiNhanh)
						AND ((select count(Name) from @tblSearchString b where 
    					hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    					or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    						or hh.TenHangHoa like '%'+b.Name+'%'
    						or lo.MaLoHang like '%' +b.Name +'%' 
    						or qd.MaHangHoa like '%'+b.Name+'%'
    						or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    						or qd.TenDonViTinh like '%'+b.Name+'%'
    						or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
							or dv.MaDonVi like '%'+b.Name+'%'
							or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)		
						order by TenHangHoa, TenDonVi,MaLoHang
			end

			
			
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_NhapXuatTonChiTiet]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
    @timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
    @CoPhatSinh [int]
AS
BEGIN
    SET NOCOUNT ON;
	DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
	INSERT INTO @tblIdDonVi
	SELECT donviinput.Name, dv.MaDonVi, dv.TenDonVi FROM [dbo].[splitstring](@ID_DonVi) donviinput
	INNER JOIN DM_DonVi dv
	ON dv.ID = donviinput.Name;

	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER);
    INSERT INTO @tblChiNhanh SELECT Name FROM splitstring(@ID_DonVi)

    DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung)
    
    DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	declare @tkDauKy table (ID_DonVi uniqueidentifier,ID_HangHoa uniqueidentifier,	ID_LoHang uniqueidentifier null, TonKho float,GiaVon float)		
	insert into @tkDauKy
	exec dbo.GetAll_TonKhoDauKy @ID_DonVi, @timeStart

	---- phatsinh trongky
	select 
		qd.ID_HangHoa,	
		tblQD.ID_LoHang,
		tblQD.ID_DonVi,
		sum(SoLuongNhap_NCC * qd.TyLeChuyenDoi) as SoLuongNhap_NCC,
		sum(SoLuongNhap_Kiem * qd.TyLeChuyenDoi) as SoLuongNhap_Kiem,
		sum(SoLuongNhap_Tra * qd.TyLeChuyenDoi) as SoLuongNhap_Tra,
		sum(SoLuongNhap_Chuyen * qd.TyLeChuyenDoi) as SoLuongNhap_Chuyen,

		sum(SoLuongXuat_Ban * qd.TyLeChuyenDoi) as SoLuongXuat_Ban,
		sum(SoLuongXuat_Huy * qd.TyLeChuyenDoi) as SoLuongXuat_Huy,
		sum(SoLuongXuat_NCC * qd.TyLeChuyenDoi) as SoLuongXuat_NCC,
		sum(SoLuongXuat_Kiem * qd.TyLeChuyenDoi) as SoLuongXuat_Kiem,
		sum(SoLuongXuat_Chuyen * qd.TyLeChuyenDoi) as SoLuongXuat_Chuyen,		
		sum(GiaTri) as GiaTri
		into #temp
	from
	(
		select 
			tblHD.ID_DonViQuiDoi,
			tblHD.ID_LoHang,
			tblHD.ID_DonVi,
			SUM(tblHD.SoLuongNhap_NCC) AS SoLuongNhap_NCC,
			SUM(tblHD.SoLuongNhap_Kiem) AS SoLuongNhap_Kiem,
			SUM(tblHD.SoLuongNhap_Tra) AS SoLuongNhap_Tra,
			SUM(tblHD.SoLuongNhap_Chuyen) AS SoLuongNhap_Chuyen,
		
			SUM(tblHD.SoLuongXuat_Ban) AS SoLuongXuat_Ban,
			SUM(tblHD.SoLuongXuat_Huy) AS SoLuongXuat_Huy,
			SUM(tblHD.SoLuongXuat_NCC) AS SoLuongXuat_NCC,
			SUM(tblHD.SoLuongXuat_Kiem) AS SoLuongXuat_Kiem,
			SUM(tblHD.SoLuongXuat_Chuyen) AS SoLuongXuat_Chuyen,		
			SUM(tblHD.GiaTri) AS GiaTri
		from
		(
				-----  banhang, xuatkho, trancc, kiem -, chuyenhang
				-- xuatban
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						SUM(ct.SoLuong) AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem,
						0 AS SoLuongXuat_Chuyen,					
						SUM(ct.SoLuong * ct.GiaVon) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (1)
					and hd.ChoThanhToan = 0 
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

					union all
					-- xuatkho
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						SUM(ct.SoLuong) AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						0 AS SoLuongXuat_Chuyen,					
						SUM(ct.SoLuong * ct.GiaVon) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (8)
					and hd.ChoThanhToan = 0 
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

					union all
					-- xuat traNCC
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						SUM(ct.SoLuong) AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						0 AS SoLuongXuat_Chuyen,						
						SUM(ct.SoLuong * ct.DonGia) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (7)
					and hd.ChoThanhToan = 0 
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

					union all
					-- xuat/nhap kiemke
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						 0 AS SoLuongNhap_NCC,
    					sum(IIF(ct.SoLuong >= 0, ct.SoLuong, 0)) AS SoLuongNhap_Kiem, 
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
    					sum(IIF(ct.SoLuong < 0, ct.SoLuong *(-1), 0)) AS SoLuongXuat_Kiem,
						0 AS SoLuongXuat_Chuyen,						
						sum((ct.ThanhTien - ct.TienChietKhau) * ct.GiaVon) as GiaTri -- soluongthucte - soluongDB (if > 0:nhap else xuat)
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (9)
					and hd.ChoThanhToan = 0 
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

				
					union all
					-- xuat chuyenhang
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						SUM(ct.TienChietKhau) AS SoLuongXuat_Chuyen,						
						SUM(ct.TienChietKhau * ct.GiaVon) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.ChoThanhToan = 0 and
						((hd.LoaiHoaDon = 10 and hd.yeucau = '1') 
						OR (hd.LoaiHoaDon = 10 and hd.YeuCau = '4'))  
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

						---- nhaphang, kiemhang, kh tra, nhanhang
					union all
					-- nhaphang
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						SUM(ct.SoLuong) AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						0 AS SoLuongXuat_Chuyen,		
						sum(case hd.LoaiHoaDon						
							when 4 then iif( hd.TongTienHang = 0,0, ct.SoLuong* (ct.DonGia - ct.TienChietKhau) * (1- hd.TongGiamGia/hd.TongTienHang)) -- sum(ThanhTien) cthd -  TongGiamGia hd
							else ct.SoLuong * ct.GiaVon end) as GiaTri
						FROM BH_HoaDon_ChiTiet ct   
						JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
						WHERE hd.LoaiHoaDon in (4,13,14)
						and hd.ChoThanhToan = 0 
						AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi


						union all
						-- kh trahang
						SELECT 
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							0 AS SoLuongNhap_NCC,
							0 AS SoLuongNhap_Kiem,
							SUM(ct.SoLuong) AS SoLuongNhap_Tra,
							0 AS SoLuongNhap_Chuyen,
							
							0 AS SoLuongXuat_Ban,
							0 AS SoLuongXuat_Huy,
							0 AS SoLuongXuat_NCC,
							0 AS SoLuongXuat_Kiem, 
							0 AS SoLuongXuat_Chuyen,							
							SUM(ct.SoLuong * iif(ctm.GiaVon is null, ct.GiaVon,ctm.GiaVon))  AS GiaTri
						FROM BH_HoaDon_ChiTiet ct   
						JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
						left join BH_HoaDon_ChiTiet ctm on ct.ID_ChiTietGoiDV = ctm.ID
						WHERE hd.LoaiHoaDon = 6  and hd.ChoThanhToan = 0
						and (ct.ChatLieu is null or ct.ChatLieu !='2')
						AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

						union all
						-- nhan chuyenhang
						select
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_CheckIn,
							0 AS SoLuongNhap_NCC,
							0 AS SoLuongNhap_Kiem,
							0 AS SoLuongNhap_Tra,
							SUM(ct.TienChietKhau) AS SoLuongNhap_Chuyen,
							
							0 AS SoLuongXuat_Ban,
							0 AS SoLuongXuat_Huy,
							0 AS SoLuongXuat_NCC,
							0 AS SoLuongXuat_Kiem, 
							0 AS SoLuongXuat_Chuyen,							
							SUM(ct.TienChietKhau * ct.DonGia) AS GiaTri -- lay dongia cua ben chuyen
					FROM BH_HoaDon_ChiTiet ct
					LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon = 10 and hd.YeuCau = '4' AND hd.ChoThanhToan = 0
					and exists (select ID from @tblChiNhanh dv where hd.ID_CheckIn = dv.ID)
					AND hd.NgaySua between @timeStart AND  @timeEnd
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_CheckIn
			)tblHD
			group by tblHD.ID_DonViQuiDoi,tblHD.ID_LoHang,tblHD.ID_DonVi
		)tblQD
		join DonViQuiDoi qd on tblQD.ID_DonViQuiDoi= qd.ID
		group by qd.ID_HangHoa, tblQD.ID_LoHang, tblQD.ID_DonVi
	
				if @CoPhatSinh= 2
					begin
						select 
							a.TenNhomHang,
							a.TenHangHoa,
							a.MaHangHoa,
							a.TenDonViTinh,
							a.TenLoHang,
							a.TenDonVi,
							a.MaDonVi,

							---- dauky
							isnull(a.TonDauKy,0) as TonDauKy,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0),0) as GiaTriDauKy,

							----- trongky
							isnull(a.SoLuongNhap_NCC,0) as SoLuongNhap_NCC,
							isnull(a.SoLuongNhap_Kiem,0) as SoLuongNhap_Kiem,
							isnull(a.SoLuongNhap_Tra,0) as SoLuongNhap_Tra,
							isnull(a.SoLuongNhap_Chuyen,0) as SoLuongNhap_Chuyen,

							isnull(a.SoLuongXuat_Ban,0) as SoLuongXuat_Ban,
							isnull(a.SoLuongXuat_Huy,0) as SoLuongXuat_Huy,
							isnull(a.SoLuongXuat_NCC,0) as SoLuongXuat_NCC,
							isnull(a.SoLuongXuat_Kiem,0) as SoLuongXuat_Kiem,
							isnull(a.SoLuongXuat_Chuyen,0) as SoLuongXuat_Chuyen,

							--cuoiky
							isnull(a.TonDauKy,0) + ( a.SoLuongNhap_NCC + a.SoLuongNhap_Kiem + SoLuongNhap_Tra + SoLuongNhap_Chuyen)
								- (SoLuongXuat_Ban + SoLuongXuat_Huy + SoLuongXuat_NCC + SoLuongXuat_Kiem + SoLuongXuat_Chuyen) as TonCuoiKy,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0) + isnull(a.GiaTri,0),0) as GiaTriCuoiKy

						from
						(
						select 
								isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
								hh.TenHangHoa,
								qd.MaHangHoa,
								concat(hh.TenHangHoa,ThuocTinhGiaTri) as TenHangHoaFull,
								isnull(ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
								qd.TenDonViTinh,
								isnull(lo.MaLoHang,'') as TenLoHang,
								dv.TenDonVi,
								dv.MaDonVi,
				
								-- dauky											
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) as TonDauKy,		
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) * iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaTriDauKy,
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaVon,	

									----trongky
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_NCC, 0) as SoLuongNhap_NCC,		
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_Kiem, 0) as SoLuongNhap_Kiem,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_Tra, 0) as SoLuongNhap_Tra,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_Chuyen, 0) as SoLuongNhap_Chuyen,

								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Ban, 0) as SoLuongXuat_Ban,		
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Huy, 0) as SoLuongXuat_Huy,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_NCC, 0) as SoLuongXuat_NCC,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Kiem, 0) as SoLuongXuat_Kiem,
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Chuyen, 0) as SoLuongXuat_Chuyen,
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.GiaTri, 0) as GiaTri


							from #temp tkTrongKy
							left join DM_HangHoa hh on tkTrongKy.ID_HangHoa= hh.ID
							left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
							left join DonViQuiDoi qd on tkTrongKy.ID_HangHoa = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
							left join DM_LoHang lo on tkTrongKy.ID_LoHang= lo.ID or lo.ID is null
							left join @tkDauKy tkDauKy 
							on hh.ID = tkDauKy.ID_HangHoa and tkDauKy.ID_DonVi= tkTrongKy.ID_DonVi and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null ))
							cross join DM_DonVi  dv									
							where hh.LaHangHoa= 1
								AND hh.TheoDoi LIKE @TheoDoi	
								and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
								and exists (select ID from @tblChiNhanh cn where dv.ID= cn.id)
									AND ((select count(Name) from @tblSearchString b where 
    								hh.TenHangHoa_KhongDau like N'%'+b.Name+'%' 
    								or hh.TenHangHoa_KyTuDau like N'%'+b.Name+'%' 
    									or hh.TenHangHoa like N'%'+b.Name+'%'
    									or lo.MaLoHang like N'%' +b.Name +'%' 
    									or qd.MaHangHoa like N'%'+b.Name+'%'
    									or nhom.TenNhomHangHoa like N'%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KhongDau like N'%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KyTuDau like N'%'+b.Name+'%'
    									or qd.TenDonViTinh like N'%'+b.Name+'%'
    									or qd.ThuocTinhGiaTri like N'%'+b.Name+'%'
										or dv.MaDonVi like N'%'+b.Name+'%'
										or dv.TenDonVi like N'%'+b.Name+'%')=@count or @count=0)		
						) a	order by TenHangHoa, TenDonVi,TenLoHang
	end
				else
				begin
				select 	
							dv.MaDonVi, dv.TenDonVi,
							qd.MaHangHoa,
							hh.TenHangHoa,
							hh.QuyCach,
							lo.ID as ID_LoHang,
							qd.ID as ID_DonViQuyDoi,
							concat(hh.TenHangHoa,ThuocTinhGiaTri) as TenHangHoaFull,
							isnull(ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
							isnull(qd.TenDonViTinh,'') as TenDonViTinh,
							isnull(lo.MaLoHang,'') as TenLoHang,

							---- dauky
							isnull(tkDauKy.TonKho,0) as TonDauKy,
							isnull(tkDauKy.GiaVon,0) as GiaVon,
							iif(@XemGiaVon ='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0),0)  as GiaTriDauKy,			
							isnull(nhom.TenNhomHangHoa,N'Nhóm Hàng Hóa Mặc Định') TenNhomHang,

							---- trongky
							isnull(SoLuongNhap_NCC,0) as SoLuongNhap_NCC,
							isnull(SoLuongNhap_Kiem,0) as SoLuongNhap_Kiem,
							isnull(SoLuongNhap_Tra,0) as SoLuongNhap_Tra,
							isnull(SoLuongNhap_Chuyen,0) as SoLuongNhap_Chuyen,

							isnull(SoLuongXuat_Ban,0) as SoLuongXuat_Ban,
							isnull(SoLuongXuat_Huy,0) as SoLuongXuat_Huy,
							isnull(SoLuongXuat_NCC,0) as SoLuongXuat_NCC,
							isnull(SoLuongXuat_Kiem,0) as SoLuongXuat_Kiem,
							isnull(SoLuongXuat_Chuyen,0) as SoLuongXuat_Chuyen,

							---- cuoiky
							isnull(tkDauKy.TonKho,0) + isnull(SoLuongNhap_NCC,0) +isnull(SoLuongNhap_Kiem,0) +  isnull(SoLuongNhap_Tra,0) + isnull(SoLuongNhap_Chuyen,0)
							- (isnull(SoLuongXuat_Ban,0) + isnull(SoLuongXuat_Huy,0) + isnull(SoLuongXuat_NCC,0) + isnull(SoLuongXuat_Kiem,0) +  isnull(SoLuongXuat_Chuyen,0)) as TonCuoiKy,
							iif(@XemGiaVon ='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0) + isnull(tkTrongKy.GiaTri,0),0)  as GiaTriCuoiKy
					from DM_HangHoa hh 		
					join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
					left join DM_LoHang lo on hh.ID = lo.ID_HangHoa
					left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
					cross join DM_DonVi  dv		
					left join @tkDauKy tkDauKy 
					on qd.ID_HangHoa = tkDauKy.ID_HangHoa and tkDauKy.ID_DonVi= dv.ID and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null ))
					left join #temp tkTrongKy on qd.ID_HangHoa = tkTrongKy.ID_HangHoa and tkTrongKy.ID_DonVi= dv.ID and ((lo.ID= tkTrongKy.ID_LoHang) or (lo.ID is null ))
					where hh.LaHangHoa= 1
					AND hh.TheoDoi LIKE @TheoDoi	
					and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
					and exists (select ID from @tblChiNhanh cn where dv.ID= cn.id)
						AND ((select count(Name) from @tblSearchString b where 
    					hh.TenHangHoa_KhongDau like N'%'+b.Name+'%' 
    					or hh.TenHangHoa_KyTuDau like N'%'+b.Name+'%' 
    						or hh.TenHangHoa like N'%'+b.Name+'%'
    						or lo.MaLoHang like N'%' +b.Name +'%' 
    						or qd.MaHangHoa like N'%'+b.Name+'%'
    						or nhom.TenNhomHangHoa like N'%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KhongDau like N'%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KyTuDau like N'%'+b.Name+'%'
    						or qd.TenDonViTinh like N'%'+b.Name+'%'
    						or qd.ThuocTinhGiaTri like N'%'+b.Name+'%'
							or dv.MaDonVi like N'%'+b.Name+'%'
							or dv.TenDonVi like N'%'+b.Name+'%')=@count or @count=0)		
						order by TenHangHoa, TenDonVi,MaLoHang
				end	
 
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_TongHopHangNhap]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
	@timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@LoaiChungTu [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;

	DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER);
	INSERT INTO @tblIdDonVi
	SELECT donviinput.Name FROM [dbo].[splitstring](@ID_DonVi) donviinput

	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung)
    
    DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	DECLARE @tblHoaDon TABLE(TenNhomHang NVARCHAR(MAX), MaHangHoa NVARCHAR(MAX), TenHangHoaFull NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), 
	ThuocTinh_GiaTri NVARCHAR(MAX), TenDonViTinh NVARCHAR(MAX), TenLoHang NVARCHAR(MAX),
	ID_DonVi UNIQUEIDENTIFIER,
	LoaiHoaDon INT, TyLeChuyenDoi FLOAT, 
	SoLuong FLOAT, TienChietKhau FLOAT, ThanhTien FLOAT, GiaVon FLOAT, YeuCau NVARCHAR(MAX), GiamGiaHDPT FLOAT);
	INSERT INTO @tblHoaDon

	SELECT nhh.TenNhomHangHoa AS TenNhomHang, dvqdChuan.MaHangHoa, 
		CONCAT(hh.TenHangHoa,dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
		hh.TenHangHoa, 
		ISNULL(dvqd.ThuocTinhGiaTri, '') AS ThuocTinh_GiaTri, 
		dvqdChuan.TenDonViTinh,
		lh.MaLoHang AS TenLoHang, 
		iif(bhd.LoaiHoaDon=10, bhd.ID_CheckIn,bhd.ID_DonVi) as ID_DonVi,
		bhd.LoaiHoaDon, dvqd.TyLeChuyenDoi, 
		bhdct.SoLuong, 
		bhdct.TienChietKhau,
		bhdct.ThanhTien, 
		case bhd.LoaiHoaDon
			when 6 then case when ctm.GiaVon is null  then bhdct.GiaVon else ctm.GiaVon end
			when 10 then bhdct.DonGia --- chuyenhang: alway get DonGia (vì GiaVon alway change)
		else bhdct.GiaVon end as GiaVon,		
		bhd.YeuCau, 
		IIF(bhd.TongTienHang = 0, 0, bhd.TongGiamGia / bhd.TongTienHang) as GiamGiaHDPT
    FROM BH_HoaDon_ChiTiet bhdct
	left join BH_HoaDon_ChiTiet ctm on bhdct.ID_ChiTietGoiDV = ctm.ID --- khachtrahang: lay giavon tu hoadonmua
    INNER JOIN BH_HoaDon bhd ON bhdct.ID_HoaDon = bhd.ID
	join @tblIdDonVi dv on (bhd.ID_DonVi = dv.ID and bhd.LoaiHoaDon!=10) or (bhd.ID_CheckIn = dv.ID and bhd.YeuCau='4')
    INNER JOIN DonViQuiDoi dvqd ON dvqd.ID = bhdct.ID_DonViQuiDoi
	INNER JOIN DonViQuiDoi dvqdChuan ON dvqdChuan.ID_HangHoa = dvqd.ID_HangHoa AND dvqdChuan.LaDonViChuan = 1
	INNER JOIN (select Name from splitstring(@LoaiChungTu)) lhd ON bhd.LoaiHoaDon = lhd.Name
	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
	INNER JOIN DM_NhomHangHoa nhh  ON nhh.ID = hh.ID_NhomHang   
    INNER JOIN (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)) allnhh   ON nhh.ID = allnhh.ID  
	LEFT JOIN DM_LoHang lh  ON lh.ID = bhdct.ID_LoHang OR (bhdct.ID_LoHang IS NULL and lh.ID is null)   
    WHERE bhd.ChoThanhToan = 0
	and (bhdct.ChatLieu is null or bhdct.ChatLieu!='2')
    AND IIF(bhd.LoaiHoaDon = 10 and bhd.YeuCau = '4', bhd.NgaySua, bhd.NgayLapHoaDon) between @timeStart and @timeEnd	
	AND hh.LaHangHoa = 1 AND hh.TheoDoi LIKE @TheoDoi AND dvqd.Xoa LIKE @TrangThai 
	AND ((select count(Name) from @tblSearchString b where 
    		hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa like '%'+b.Name+'%'
    			or lh.MaLoHang like '%' +b.Name +'%' 
    			or dvqd.MaHangHoa like '%'+b.Name+'%'
				or dvqdChuan.MaHangHoa like '%'+b.Name+'%'
    			or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    			or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    			or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    			or dvqd.TenDonViTinh like '%'+b.Name+'%'
    			or dvqd.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)	
	

	SELECT pstk.TenNhomHang, pstk.MaHangHoa, pstk.TenHangHoaFull, pstk.TenHangHoa, pstk.ThuocTinh_GiaTri,
	pstk.TenDonViTinh, pstk.TenLoHang,
	dv.TenDonVi, dv.MaDonVi, 
	SUM(pstk.SoLuongNhap) AS SoLuong, IIF(@XemGiaVon = '1',ROUND(SUM(pstk.GiaTriNhap),0) , 0) as ThanhTien
	FROM 
	(
	-- hoadon
    SELECT 
    TenNhomHang, MaHangHoa, TenHangHoaFull, TenHangHoa, ThuocTinh_GiaTri, TenDonViTinh, TenLoHang, ID_DonVi,
	IIF(LoaiHoaDon = 10 and YeuCau = '4', TienChietKhau* TyLeChuyenDoi, SoLuong * TyLeChuyenDoi) AS SoLuongNhap,
	case LoaiHoaDon
		when 4 then ThanhTien*(1-GiamGiaHDPT)
		when 10 then iif(YeuCau = '4', TienChietKhau* GiaVon,0)
	else SoLuong * GiaVon end as GiaTriNhap		
    FROM @tblHoaDon WHERE LoaiHoaDon != 9

	UNION ALL
    SELECT 
	-- phieukiemke
    TenNhomHang, MaHangHoa, TenHangHoaFull, TenHangHoa, ThuocTinh_GiaTri, TenDonViTinh, TenLoHang, ID_DonVi,
	Sum(SoLuong * TyLeChuyenDoi) as SoLuongNhap,	
	SUM(SoLuong * TyLeChuyenDoi * GiaVon) AS GiaTriNhap
    FROM @tblHoaDon
    WHERE LoaiHoaDon = 9 and SoLuong > 0 --- chi lay chitiet kiemke neu soluonglech tang
    GROUP BY TenNhomHang, MaHangHoa, TenHangHoaFull, TenHangHoa, ThuocTinh_GiaTri, TenDonViTinh, TenLoHang, ID_DonVi
	) pstk
	join DM_DonVi dv on pstk.ID_DonVi= dv.ID
	GROUP BY pstk.TenNhomHang, pstk.MaHangHoa, pstk.TenHangHoaFull, pstk.TenHangHoa, pstk.ThuocTinh_GiaTri, pstk.TenDonViTinh, pstk.TenLoHang,
	pstk.ID_DonVi, dv.TenDonVi, dv.MaDonVi
	order by MaHangHoa

END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_TongHopHangXuat]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
	@timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@LoaiChungTu [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
	 DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER);
    INSERT INTO @tblChiNhanh SELECT Name FROM splitstring(@ID_DonVi)

	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung)
    
    DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

			select 
				dv.MaDonVi, dv.TenDonVi,
				isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
				isnull(lo.MaLoHang,'') as TenLoHang,
				qd.MaHangHoa, qd.TenDonViTinh, 
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				hh.TenHangHoa,
				CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,
				round(SoLuong,3) as SoLuong,
				iif(@XemGiaVon='1', round(ThanhTien,3),0) as ThanhTien
			from
			(				
				select 
					qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi,
					sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
					sum(GiaTriXuat) as ThanhTien
				from
				(
						select 
							hd.NgayLapHoaDon, 
							hd.MaHoaDon,
							hd.LoaiHoaDon,
							hd.ID_HoaDon,
							case hd.LoaiHoaDon
								when 8 then case when hd.ID_PhieuTiepNhan is not null then case when ct.ChatLieu= 4 then 2 else 11 end -- xuat suachua
									else 8 end
								when 1 then case when hd.ID_HoaDon is null and ct.ID_ChiTietGoiDV is not null then 2 --- xuat sudung gdv
									else case when ct.ID_ChiTietDinhLuong is not null then 3 --- xuat ban dinhluong
									else 1 end end -- xuat banle
								else hd.LoaiHoaDon end as LoaiXuatKho, -- xuat khac: traNCC, chuyenang,..
							ct.ID_ChiTietGoiDV,
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							case hd.LoaiHoaDon
							when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0)
							when 10 then ct.TienChietKhau else ct.SoLuong end as SoLuong,
							ct.TienChietKhau,
							ct.GiaVon,
							case hd.LoaiHoaDon
								when 7 then ct.SoLuong * ct.DonGia
								when 10 then ct.TienChietKhau * ct.DonGia
								when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0) * ct.GiaVon
								else ct.SoLuong* ct.GiaVon end as GiaTriXuat
						from BH_HoaDon_ChiTiet ct
						join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
						WHERE hd.ChoThanhToan = 0 
						AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						and hd.LoaiHoaDon not in (3,4,6,19,25)
						and iif(hd.LoaiHoaDon=9,ct.SoLuong, -1) < 0 -- phieukiemke: chi lay neu soluong < 0 (~xuatkho)
				) tblHD
				join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID			
				where exists (select Name from dbo.splitstring(@LoaiChungTu) loaict where tblHD.LoaiXuatKho = loaict.Name)				
				group by qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi		
			)tblQD
			join DM_DonVi dv on tblQD.ID_DonVi = dv.ID
			join DM_HangHoa hh on tblQD.ID_HangHoa= hh.ID
			join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
			left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
			left join DM_LoHang lo on tblQD.ID_LoHang= lo.ID and (lo.ID= tblQD.ID_LoHang or (tblQD.ID_LoHang is null and lo.ID is null))
			where hh.LaHangHoa = 1
			and hh.TheoDoi like @TheoDoi
			and qd.Xoa like @TrangThai
			and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID= allnhh.ID)
			AND ((select count(Name) from @tblSearchString b where 
    		hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa like '%'+b.Name+'%'
    		or lo.MaLoHang like '%' +b.Name +'%' 
			or qd.MaHangHoa like '%'+b.Name+'%'
			or qd.MaHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    		or qd.TenDonViTinh like '%'+b.Name+'%'
    		or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
			or dv.MaDonVi like '%'+b.Name+'%'
			or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
		order by dv.TenDonVi, hh.TenHangHoa, lo.MaLoHang	


END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_XuatChuyenHang]
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang UNIQUEIDENTIFIER,
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	 SET NOCOUNT ON;
	 DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER);
	 INSERT INTO @tblIdDonVi
	 SELECT Name FROM [dbo].[splitstring](@ID_ChiNhanh) 

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	declare @tblNhomHang table(ID UNIQUEIDENTIFIER)
	insert into @tblNhomHang
	SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From HT_NguoiDung nd	    		
    		where nd.ID = @ID_NguoiDung);

			select 
				dv.MaDonVi, dv.TenDonVi,
				isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
				isnull(lo.MaLoHang,'') as TenLoHang,
				qd.MaHangHoa, qd.TenDonViTinh, 
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				hh.TenHangHoa,
				CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,
				tblQD.SoLuong,
				iif(@XemGiaVon='1', ThanhTien,0) as ThanhTien
			from
			(
				select 
					qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi,
					sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
					sum(ThanhTien) as ThanhTien
				from(
				select ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi,
					sum(ct.TienChietKhau) as SoLuong,
					sum(ct.TienChietKhau * ct.GiaVon) as ThanhTien
				from BH_HoaDon_ChiTiet ct
				join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
				where hd.ChoThanhToan=0
				and hd.LoaiHoaDon= 10 and (hd.YeuCau='1' or hd.YeuCau='4') --- YeuCau: 1.DangChuyen, 4.DaNhan, 2.PhieuTam, 3.Huy
				and hd.NgayLapHoaDon between @timeStart and @timeEnd
				and exists (select ID from @tblIdDonVi dv where hd.ID_DonVi= dv.ID)
				group by ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi
				)tblHD
				join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID
				group by qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi
			)tblQD
			join DM_DonVi dv on tblQD.ID_DonVi = dv.ID
			join DM_HangHoa hh on tblQD.ID_HangHoa= hh.ID
			join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
			left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
			left join DM_LoHang lo on tblQD.ID_LoHang= lo.ID and (lo.ID= tblQD.ID_LoHang or (tblQD.ID_LoHang is null and lo.ID is null))
			where hh.LaHangHoa = 1
			and hh.TheoDoi like @TheoDoi
			and qd.Xoa like @TrangThai
			and exists (SELECT ID FROM @tblNhomHang allnhh where nhom.ID= allnhh.ID)
			AND ((select count(Name) from @tblSearchString b where 
    		hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa like '%'+b.Name+'%'
    		or lo.MaLoHang like '%' +b.Name +'%' 
			or qd.MaHangHoa like '%'+b.Name+'%'
			or qd.MaHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    		or qd.TenDonViTinh like '%'+b.Name+'%'
    		or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
			or dv.MaDonVi like '%'+b.Name+'%'
			or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
		order by dv.TenDonVi, hh.TenHangHoa, lo.MaLoHang	
    
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_XuatChuyenHangChiTiet]
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang UNIQUEIDENTIFIER,
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	 SET NOCOUNT ON;
	 DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER);
	 INSERT INTO @tblIdDonVi
	 SELECT Name FROM [dbo].[splitstring](@ID_ChiNhanh) 

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From HT_NguoiDung nd	    		
    		where nd.ID = @ID_NguoiDung);


				select 
					CNChuyen.TenDonVi as ChiNhanhChuyen,
					CNnhan.TenDonVi as ChiNhanhNhan,
					tblHD.NgayLapHoaDon,
					tblHD.MaHoaDon,
					isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
					isnull(lo.MaLoHang,'') as TenLoHang,
					qd.MaHangHoa, qd.TenDonViTinh, 
					isnull(qd.ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
					hh.TenHangHoa,
					CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,				
					tblHD.SoLuong as SoLuong,
					iif(@XemGiaVon='1',tblHD.DonGia,0) as DonGia,
					iif(@XemGiaVon='1',tblHD.GiaVon,0) as GiaVon,
					iif(@XemGiaVon='1',tblHD.ThanhTien,0) as ThanhTien,
					iif(@XemGiaVon='1',tblHD.GiaTri,0) as GiaTri			
				from(
					select 
						qd.ID_HangHoa,tblHD.ID_LoHang, 
						tblHD.ID_DonVi,tblHD.ID_CheckIn, tblHD.NgayLapHoaDon,tblHD.MaHoaDon,
						sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
						max(tblHD.GiaVon / iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as GiaVon,
						max(tblHD.DonGia / iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as DonGia,
						sum(tblHD.ThanhTien) as ThanhTien,
						sum(tblHD.GiaTri) as GiaTri
					from(
					select ct.ID_DonViQuiDoi, ct.ID_LoHang, 
						hd.ID_DonVi, hd.ID_CheckIn, hd.NgayLapHoaDon,hd.MaHoaDon,
						sum(ct.TienChietKhau) as SoLuong,
						max(ct.GiaVon) as GiaVon,
						max(ct.DonGia) as DonGia,
						sum(ct.DonGia * ct.SoLuong) as ThanhTien,
						sum(ct.TienChietKhau * ct.GiaVon) as GiaTri
					from BH_HoaDon_ChiTiet ct
					join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
					where hd.ChoThanhToan=0
					and hd.LoaiHoaDon= 10 and (hd.YeuCau='1' or hd.YeuCau='4') --- YeuCau: 1.DangChuyen, 4.DaNhan, 2.PhieuTam, 3.Huy
					and hd.NgayLapHoaDon between @timeStart and @timeEnd
					and exists (select ID from @tblIdDonVi dv where hd.ID_DonVi= dv.ID)
					group by ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi, hd.ID_CheckIn,hd.NgayLapHoaDon, hd.MaHoaDon
					)tblHD
					join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID
					group by qd.ID_HangHoa, tblHD.ID_DonViQuiDoi,tblHD.ID_LoHang, tblHD.ID_DonVi,tblHD.ID_CheckIn,tblHD.NgayLapHoaDon,tblHD.MaHoaDon
				)tblHD
				join DM_DonVi CNChuyen on tblHD.ID_DonVi = CNChuyen.ID
				left join DM_DonVi CNnhan on tblHD.ID_CheckIn= CNnhan.ID
				join DM_HangHoa hh on tblHD.ID_HangHoa= hh.ID
				join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
				left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
				left join DM_LoHang lo on tblHD.ID_LoHang= lo.ID and (lo.ID= tblHD.ID_LoHang or (tblHD.ID_LoHang is null and lo.ID is null))
				where hh.LaHangHoa = 1 
				and hh.TheoDoi like @TheoDoi
				and qd.Xoa like @TrangThai
				and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID= allnhh.ID)
				AND ((select count(Name) from @tblSearchString b where 
    			hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa like '%'+b.Name+'%'
    			or lo.MaLoHang like '%' +b.Name +'%' 
				or qd.MaHangHoa like '%'+b.Name+'%'
				or qd.MaHangHoa like '%'+b.Name+'%'
    			or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    			or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    			or qd.TenDonViTinh like '%'+b.Name+'%'
    			or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
				or CNnhan.TenDonVi like '%'+b.Name+'%'
				or CNChuyen.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
				order by CNChuyen.TenDonVi, tblHD.NgayLapHoaDon desc, hh.TenHangHoa, lo.MaLoHang 
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoNhapHang_ChiTiet]
    @Text_Search [nvarchar](max),  
    @MaNCC [nvarchar](max),   
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @ID_NhomNCC [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
set nocount on;
if @MaNCC is null set @MaNCC =''

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)

			DECLARE @tblSearchString TABLE (Name [nvarchar](max));
			DECLARE @count int;
			INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
			Select @count =  (Select count(*) from @tblSearchString);


	   SELECT 
    		a.MaHoaDon,
    		a.NgayLapHoaDon,
    		a.MaNhaCungCap,
    		a.TenNhaCungCap,
    		a.MaHangHoa,
    		a.TenHangHoaFull,
    		a.TenHangHoa,
    		a.ThuocTinh_GiaTri,
    		a.TenDonViTinh,
    		a.TenLoHang,
			a.TienThue,
    		a.SoLuong, 
    		Case When @XemGiaVon = '1' then a.GiaBan else 0 end  as DonGia,
    		Case When @XemGiaVon = '1' then	a.TienChietKhau else 0 end  as TienChietKhau,
    		Case When @XemGiaVon = '1' then a.ThanhTien else 0 end  as ThanhTien,
    		Case When @XemGiaVon = '1' then	a.GiamGiaHD else 0 end  as GiamGiaHD,
    		Case When @XemGiaVon = '1' then a.ThanhTien - a.GiamGiaHD else 0 end as GiaTriNhap, 
    		a.TenNhanVien,
			a.GhiChu
    	FROM
    	(
    		Select hd.MaHoaDon,
    		hd.NgayLapHoaDon,
    		Case When dtn.ID_NhomDoiTuong is null then '30000000-0000-0000-0000-000000000003' else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    		dt.MaDoiTuong as MaNhaCungCap,
    		dt.TenDoiTuong as TenNhaCungCap,
    		dvqd.MaHangHoa,
    		concat(hh.TenHangHoa ,' ', dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
    		hh.TenHangHoa,
    		dvqd.TenDonViTinh,
    		dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		lh.MaLoHang as TenLoHang,
    		hdct.SoLuong,
    		hdct.DonGia as GiaBan,
    		hdct.TienChietKhau,
    		nv.TenNhanVien,
			hdct.GhiChu,
    		hdct.ThanhTien,
			hdct.TienThue * hdct.SoLuong as TienThue,
			Case when hd.TongTienHang = 0 then 0 else hdct.ThanhTien * (hd.TongGiamGia / hd.TongTienHang) end as GiamGiaHD
    		FROM BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
    		inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		left join DM_LoHang lh on hdct.ID_LoHang = lh.ID
    		left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
    		left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
    		where hd.NgayLapHoaDon between @timeStart and @timeEnd
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    		and hd.ChoThanhToan = 0 
    		and hd.LoaiHoaDon in (4,13,14)
    		and dvqd.Xoa like @TrangThai
    		and hh.TheoDoi like @TheoDoi
			and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where hh.ID_NhomHang= allnhh.ID))    		
			AND
			((select count(Name) from @tblSearchString b where 
    				hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa like '%'+b.Name+'%'
    				or lh.MaLoHang like '%' +b.Name +'%' 
    				or dvqd.MaHangHoa like '%'+b.Name+'%'
    				or hd.MaHoaDon like '%'+b.Name+'%'
    				or hdct.GhiChu like '%'+b.Name+'%'    									
    				or dvqd.TenDonViTinh like '%'+b.Name+'%'
    				or dvqd.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)
    		and  (dt.MaDoiTuong like N'%'+@MaNCC +'%'
					or dt.TenDoiTuong_KhongDau like N'%'+ @MaNCC+'%'
					or dt.TenDoiTuong like N'%'+ @MaNCC+'%'
					or dt.TenDoiTuong_ChuCaiDau like N'%'+ @MaNCC +'%'
					or dt.DienThoai like N'%'+ @MaNCC +'%')
    	) a
    	where (@ID_NhomNCC ='' or a.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomNCC)))
    	order by a.NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoNhapHang_NhomHang]
    @TextSearch [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
set nocount on;
		

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)

			DECLARE @tblSearchString TABLE (Name [nvarchar](max));
			DECLARE @count int;
			INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
			Select @count =  (Select count(*) from @tblSearchString);

		SELECT 
    		Max(a.TenNhomHangHoa) as TenNhomHangHoa,
			sum(a.TienThue) as TienThue,
    		SUM(a.SoLuong) as SoLuongNhap, 
    		Case When @XemGiaVon = '1' then Sum(a.ThanhTien) else 0 end as ThanhTien,
    		Case When @XemGiaVon = '1' then  Sum(a.GiamGiaHD) else 0 end as GiamGiaHD,
    		Case When @XemGiaVon = '1' then  Sum(a.ThanhTien - a.GiamGiaHD) else 0 end as GiaTriNhap
    	FROM
    	(
    		Select
    		nhh.TenNhomHangHoa as TenNhomHangHoa,
			hh.ID_NhomHang,
    		hdct.SoLuong,
			hdct.SoLuong * hdct.TienThue as TienThue,
    		hdct.ThanhTien,
			Case when hd.TongTienHang = 0 then 0 else hdct.ThanhTien * (hd.TongGiamGia / hd.TongTienHang) end as GiamGiaHD
    		FROM BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
    		inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID
    		left join DM_LoHang lh on hdct.ID_LoHang = lh.ID
    		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
    		where hd.NgayLapHoaDon between @timeStart and  @timeEnd
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    		and hd.ChoThanhToan = 0 
    		and hd.LoaiHoaDon in (4,13,14)
			and dvqd.Xoa like @TrangThai
    		and hh.TheoDoi like @TheoDoi
			and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where hh.ID_NhomHang= allnhh.ID)) 
			 AND
				((select count(Name) from @tblSearchString b where 
    					nhh.TenNhomHangHoa like '%'+b.Name+'%' 
    					or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'    							
    					)=@count or @count=0)	
    	) a
    	Group by a.ID_NhomHang
    	OrDER BY GiaTriNhap DESC
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoNhapHang_TheoNhaCungCap]
    @TextSearch [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
	@ID_NhomNCC [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)

			DECLARE @tblSearchString TABLE (Name [nvarchar](max));
			DECLARE @count int;
			INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
			Select @count =  (Select count(*) from @tblSearchString);

    SELECT b.NhomKhachHang,
    	b.MaNhaCungCap,
    	b.TenNhaCungCap,
    	b.DienThoai,
    	b.SoLuongNhap, b.ThanhTien, b.GiamGiaHD, b.GiaTriNhap, b.TienThue
    	 FROM
    	(
    		SELECT
    			Case When DoiTuong_Nhom.ID_NhomDoiTuong is null then '30000000-0000-0000-0000-000000000003' else DoiTuong_Nhom.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    			Case when DoiTuong_Nhom.TenNhomDT is not null then DoiTuong_Nhom.TenNhomDT else N'Nhóm mặc định' end as NhomKhachHang,
    			dt.MaDoiTuong AS MaNhaCungCap,
    			dt.TenDoiTuong AS TenNhaCungCap,
    			Case when dt.DienThoai is null then '' else dt.DienThoai end AS DienThoai,
    			a.SoLuongNhap,
    			a.ThanhTien,    				
    			a.GiamGiaHD,
    			a.GiaTriNhap,
				a.TienThue
    		FROM
    		(
    			SELECT
    				NCC.ID_NhaCungCap,
					sum(TienThue) as TienThue,
    				SUM(NCC.SoLuong) as SoLuongNhap, 
    				Case When @XemGiaVon = '1' then Sum(NCC.ThanhTien) else 0 end as ThanhTien,
    				Case When @XemGiaVon = '1' then Sum(NCC.GiamGiaHD) else 0 end as GiamGiaHD,
    				Case When @XemGiaVon = '1' then Sum(NCC.ThanhTien - NCC.GiamGiaHD) else 0 end as GiaTriNhap
    			FROM
    			(
    				SELECT
    				hd.ID_DoiTuong as ID_NhaCungCap,
					hdct.TienThue * hdct.SoLuong as TienThue,
    				ISNULL(hdct.SoLuong, 0) AS SoLuong,
    				ISNULL(hdct.ThanhTien, 0) AS ThanhTien,
					Case when hd.TongTienHang = 0 then 0 else hdct.ThanhTien * (hd.TongGiamGia / hd.TongTienHang) end as GiamGiaHD
    				FROM
    				BH_HoaDon hd 
    				inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
    				inner join DonViQuiDoi dvqd on dvqd.ID = hdct.ID_DonViQuiDoi
    				inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    				left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
    				where hd.NgayLapHoaDon between  @timeStart and @timeEnd
    				and hd.ChoThanhToan = 0
    				and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    				and hd.LoaiHoaDon in (4,13,14)
    				and hh.TheoDoi like @TheoDoi
					and dvqd.Xoa like @TrangThai
					and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where hh.ID_NhomHang= allnhh.ID))
					AND
						((select count(Name) from @tblSearchString b where 
    							dt.MaDoiTuong like '%'+b.Name+'%' 
    							or dt.TenDoiTuong like '%'+b.Name+'%' 
    							or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    							or dt.DienThoai like '%' +b.Name +'%' 
    							)=@count or @count=0)								
    			) AS NCC
    			Group by NCC.ID_NhaCungCap
    		) a
    		left join DM_DoiTuong dt on a.ID_NhaCungCap = dt.ID
    		Left join (Select Main.ID as ID_DoiTuong,
    					Left(Main.dt_n,Len(Main.dt_n)-1) As TenNhomDT,
    					Left(Main.id_n,Len(Main.id_n)-1) As ID_NhomDoiTuong
    					From
    					(
    					Select distinct hh_tt.ID,
    					(
    						Select ndt.TenNhomDoiTuong + ', ' AS [text()]
    						From dbo.DM_DoiTuong_Nhom dtn
    						inner join DM_NhomDoiTuong ndt on dtn.ID_NhomDoiTuong = ndt.ID
    						Where dtn.ID_DoiTuong = hh_tt.ID
    						order by ndt.TenNhomDoiTuong
    						For XML PATH ('')
    					) dt_n,
    					(
    					Select convert(nvarchar(max), ndt.ID)  + ', ' AS [text()]
    					From dbo.DM_DoiTuong_Nhom dtn
    					inner join DM_NhomDoiTuong ndt on dtn.ID_NhomDoiTuong = ndt.ID
    					Where dtn.ID_DoiTuong = hh_tt.ID
    					order by ndt.TenNhomDoiTuong
    					For XML PATH ('')
    					) id_n
    					From dbo.DM_DoiTuong hh_tt
    					) Main) as DoiTuong_Nhom on dt.ID = DoiTuong_Nhom.ID_DoiTuong
    	) b
    	where (@ID_NhomNCC ='' or b.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomNCC)))
    	ORDER BY GiaTriNhap DESC
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoNhapHang_TongHop]
    @Text_Search [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	set nocount on	
    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)

			DECLARE @tblSearchString TABLE (Name [nvarchar](max));
			DECLARE @count int;
			INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
			Select @count =  (Select count(*) from @tblSearchString);

	 SELECT 
    		Max(a.TenNhomHangHoa) as TenNhomHangHoa,
    		a.MaHangHoa,
    		Max(a.TenHangHoaFull) as  TenHangHoaFull,
    		Max(a.TenHangHoa) as TenHangHoa,
    		Max(a.ThuocTinh_GiaTri) as ThuocTinh_GiaTri,
    		Max(a.TenDonViTinh) as TenDonViTinh,
    		a.TenLoHang,
			sum(a.TienThue) as TienThue,
    		SUM(a.SoLuong) as SoLuong, 
    		Case When @XemGiaVon = '1' then Sum(a.ThanhTien)  else 0 end as ThanhTien,
    		Case When @XemGiaVon = '1' then Sum(a.GiamGiaHD)  else 0 end as GiamGiaHD,
    		Case When @XemGiaVon = '1' then Sum(a.ThanhTien - a.GiamGiaHD) else 0 end as GiaTriNhap
    	FROM
    	(
    		Select 
    		Case when dvqd.Xoa is null then 0 else dvqd.Xoa end as Xoa,
    		nhh.TenNhomHangHoa as TenNhomHangHoa,
    		dvqd.MaHangHoa,
			CONCAT(hh.TenHangHoa, dvqd.ThuocTinhGiaTri) as TenHangHoaFull,    		
    		hh.TenHangHoa,
    		dvqd.TenDonViTinh  as TenDonViTinh,
    		dvqd.ThuocTinhGiaTri  as ThuocTinh_GiaTri,
    		lh.MaLoHang as TenLoHang,
    		Case When hdct.ID_LoHang is not null then hdct.ID_LoHang else '10000000-0000-0000-0000-000000000001' end as ID_LoHang,
    		hdct.SoLuong,
			hdct.SoLuong * hdct.TienThue as TienThue,
    		hdct.DonGia - hdct.TienChietKhau as GiaBan,
    		Case When @XemGiaVon = '1' then ISNULL(hdct.GiaVon, 0) else 0 end as GiaVon, 
    		hdct.ThanhTien,
			Case when hd.TongTienHang = 0 then 0 else hdct.ThanhTien * (hd.TongGiamGia / hd.TongTienHang) end as GiamGiaHD
    		FROM BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
    		inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID
    		left join DM_LoHang lh on hdct.ID_LoHang = lh.ID
    		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
    		where hd.NgayLapHoaDon between @timeStart and  @timeEnd
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    		and hd.ChoThanhToan = 0 
    		and hd.LoaiHoaDon in (4,13,14)
    		and hh.TheoDoi like @TheoDoi
			and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)	)
    		and dvqd.Xoa like @TrangThai
			AND
		((select count(Name) from @tblSearchString b where 
    			 hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa like '%'+b.Name+'%'
    				or lh.MaLoHang like '%' +b.Name +'%' 
    				or dvqd.MaHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    				or dvqd.TenDonViTinh like '%'+b.Name+'%'
    				or dvqd.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)
    	) a
    	Group by a.MaHangHoa, a.TenLoHang, a.ID_LoHang, a.TenHangHoa
    	OrDER BY a.TenHangHoa
END");

			Sql(@"ALTER PROCEDURE [dbo].[Gara_JqAutoHangHoa]
    @ID_ChiNhanh [uniqueidentifier],
    @ID_BangGia [nvarchar](40),
    @TextSearch [nvarchar](200),
    @LaHangHoa [nvarchar](10),
    @QuanLyTheoLo [nvarchar](10),
    @ConTonKho int,
	@Form [int], ---- 1.nhaphang, 0.other
	@CurrentPage [int],
	@PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
	declare @dtNow datetime = DATEADD(SECOND, -1, FORMAT(getdate(),'yyyy-MM-dd'))

	declare @txtSeachUnsign nvarchar(max) = (select dbo.FUNC_ConvertStringToUnsign(@TextSearch));    
	set @TextSearch = CONCAT('%',@TextSearch,'%')
    
    DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	with data_cte
	as 
	(
	select tbView.*,
		ISNULL(anh.URLAnh,'') as SrcImage
	from
	(
		select --ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS RN,
    		ID_DonViQuiDoi,
			ID,
			ID_LoHang,
			ID_NhomHangHoa,
			ID_Xe,
			LaHangHoa,
    		MaHangHoa,TenHangHoa, TenDonViTinh,TyLeChuyenDoi,MaLoHang, NgaySanXuat, NgayHetHan,
    		ThuocTinhGiaTri,LaDonViChuan,
			LoaiHangHoa,
			TonKho,
			GiaNhap,
			isnull(GiaBan2, GiaBan) as GiaBan,	
			iif(c.LaHangHoa= 1, c.GiaVon, dbo.GetGiaVonOfDichVu(@ID_ChiNhanh,c.ID_DonViQuiDoi)) as GiaVon,   
			isnull(TenNhomHangHoa,N'Nhóm mặc định') as TenNhomHangHoa,
			Case when LaHangHoa='1' then 0 else CAST(ISNULL(ChiPhiThucHien,0) as float) end as PhiDichVu,
    		Case when LaHangHoa='1' then '0' else ISNULL(ChiPhiTinhTheoPT,'0') end as LaPTPhiDichVu,				
			isnull(QuanLyBaoDuong,0) as QuanLyBaoDuong,
			case when ISNULL(QuyCach,0) = 0 then TyLeChuyenDoi else QuyCach * TyLeChuyenDoi end as QuyCach,
			ISNULL(DonViTinhQuyCach,'0') as DonViTinhQuyCach,
    		ISNULL(QuanLyTheoLoHang,'0') as QuanLyTheoLoHang,
    		ISNULL(ThoiGianBaoHanh,0) as ThoiGianBaoHanh,
    		ISNULL(LoaiBaoHanh,0) as LoaiBaoHanh,
    		ISNULL(SoPhutThucHien,0) as SoPhutThucHien, 
    		ISNULL(GhiChu,'') as GhiChuHH ,
    		ISNULL(DichVuTheoGio,0) as DichVuTheoGio, 
    		ISNULL(DuocTichDiem,0) as DuocTichDiem,    
			ISNULL(HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,   
    		CONCAT(MaHangHoa, ' ', lower(MaHangHoa),' ', TenHangHoa, ' ', TenHangHoa_KhongDau,' ',
    		MaLoHang, ' ', GiaBan, ' ', ThuocTinhGiaTri) as Name
    from(
	select a.*, b.GiaBan2
	from
	(
	select 
		tbl.*,
		isnull(tk.TonKho, 0) as TonKho,
    	isnull(gv.GiaVon, 0) as GiaVon
	from
	(
	select 
		
		hh.ID, 
		hh.ID_Xe,
		hh.TenHangHoa,
		hh.ID_NhomHang as ID_NhomHangHoa,
		hh.LaHangHoa,		
		hh.QuanLyBaoDuong,
		hh.QuanLyTheoLoHang,
		hh.TenHangHoa_KhongDau,
		hh.ChiPhiThucHien,
		hh.ChiPhiTinhTheoPT,
		hh.DonViTinhQuyCach,
		hh.QuyCach,
		hh.ThoiGianBaoHanh,
		hh.LoaiBaoHanh,
		hh.SoPhutThucHien,
		hh.GhiChu,
		hh.DichVuTheoGio,
		hh.DuocTichDiem,
		hh.HoaHongTruocChietKhau,
		nhom.TenNhomHangHoa,
		qd.ID as ID_DonViQuiDoi, 
		qd.MaHangHoa,
		qd.TenDonViTinh, 
		qd.ThuocTinhGiaTri, 
		qd.TyLeChuyenDoi, 
		qd.GiaBan, 
		qd.GiaNhap,
		qd.LaDonViChuan,
		lo.ID as ID_LoHang, 
		lo.MaLoHang,
		lo.NgaySanXuat,
		lo.NgayHetHan,
		iif(hh.LoaiHangHoa is null, iif(LaHangHoa = 1,1,2),hh.LoaiHangHoa) as LoaiHangHoa
	from DM_HangHoa hh
	join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa
	left join DM_NhomHangHoa nhom on hh.ID_NhomHang = nhom.ID
	left join DM_LoHang lo on hh.ID= lo.ID_HangHoa
	where  hh.TheoDoi = '1'
			and (qd.xoa ='0'  or qd.Xoa is null)   			
		and (@Form=1 or (lo.NgayHetHan is null or lo.NgayHetHan > @dtNow)) ---- nhaphang
	and (
		hh.TenHangHoa like @TextSearch
		or hh.TenHangHoa like @txtSeachUnsign
		or hh.TenHangHoa_KhongDau like @TextSearch
		or hh.TenHangHoa_KhongDau like @txtSeachUnsign
		or qd.MaHangHoa like @TextSearch
		or qd.MaHangHoa like @txtSeachUnsign
		or lo.MaLoHang like @TextSearch
		or lo.MaLoHang like @txtSeachUnsign
		or 
		(
		(select count(Name) from @tblSearchString b where     			
    					hh.TenHangHoa like '%'+b.Name+'%'
    					or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'
    					or qd.MaHangHoa like '%'+b.Name+'%'		
    					or lo.MaLoHang like '%'+b.Name+'%'		
						or nhom.TenNhomHangHoa like '%'+b.Name+'%'	
						or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'	
    					)=@count or @count=0
		)
	)	
    ) tbl
	left join DM_HangHoa_TonKho tk on tk.ID_DonViQuyDoi= tbl.ID_DonViQuiDoi and (tbl.ID_LoHang = tk.ID_LoHang or tbl.ID_LoHang is null) and tk.ID_DonVi= @ID_ChiNhanh
    left join DM_GiaVon gv on tbl.ID_DonViQuiDoi = gv.ID_DonViQuiDoi and (tbl.ID_LoHang = gv.ID_LoHang or tbl.ID_LoHang is null) and gv.ID_DonVi= @ID_ChiNhanh
	where  (@Form = 1 or (tbl.QuanLyTheoLoHang='0' or (tbl.QuanLyTheoLoHang='1'  and tbl.MaLoHang!='')))
	and 
	(
	tbl.LaHangHoa like @LaHangHoa 
		or (case tbl.LoaiHangHoa 
			when 1 then 11
			when 2 then 12
			when 3 then 23 end) like @LaHangHoa)
	) a
	left join
    	(			
    		select ct.ID_DonViQuiDoi, ct.GiaBan as GiaBan2
    		from DM_GiaBan_ChiTiet ct where ct.ID_GiaBan = @ID_BangGia
    	) b on a.ID_DonViQuiDoi= b.ID_DonViQuiDoi
	 where a.LaHangHoa = 0 or a.TonKho > iif(@Contonkho='1', 0, -99999)
   ) c 	
   ) tbView
   left join DM_HangHoa_Anh anh on tbView.ID= anh.ID_HangHoa and anh.SoThuTu = 1
   )
	select dt.*
	from data_cte dt
	order by dt.NgayHetHan
	OFFSET (@CurrentPage* @PageSize) ROWS
	FETCH NEXT @PageSize ROWS ONLY; 

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetAll_TonKhoDauKy]
    @IDDonVis [nvarchar](max),
    @ToDate [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER);
    	INSERT INTO @tblChiNhanh SELECT Name FROM splitstring(@IDDonVis)

		select 
				ID_DonViInput,			
				ID_HangHoa,		
    			ID_LoHang,
				TonLuyKe,
				GiaVon
		from
		(
		select 
				tblUnion.ID_DonViInput, 
				tblUnion.LoaiHoaDon,
				tblUnion.NgayLapHoaDon,
				dvqd.ID_HangHoa,
				tblUnion.ID_LoHang,
				tblUnion.TonLuyKe,
				tblUnion.GiaVon / ISNULL(dvqd.TyLeChuyenDoi,1) as GiaVon,
				ROW_NUMBER() OVER (PARTITION BY dvqd.ID_HangHoa, tblUnion.ID_LoHang, tblUnion.ID_DonViInput ORDER BY tblUnion.NgayLapHoaDon DESC) AS RN
		from
		(
			SELECT hd.ID_DonVi as ID_DonViInput, 					
					hd.LoaiHoaDon,
					hd.NgayLapHoaDon,					
					hdct.ID_DonViQuiDoi,
					hdct.ID_LoHang,
					hdct.TonLuyKe,
					hdct.GiaVon
    		FROM BH_HoaDon_ChiTiet hdct
    		JOIN BH_HoaDon hd ON hd.ID = hdct.ID_HoaDon	
    		where hd.ChoThanhToan = 0  
			AND hd.LoaiHoaDon IN (1, 5, 7, 8, 4, 6, 9,18, 13,14)
    		and hd.ID_DonVi in (select ID from @tblChiNhanh dv)
			and hd.NgayLapHoaDon < @ToDate

			union all

			SELECT
					cn.ID as ID_DonViInput,
					hd.LoaiHoaDon,
					IIF(ID_CheckIn = cn.ID and hd.YeuCau='4', hd.NgaySua, hd.NgayLapHoaDon) as NgayLapHoaDon,
					hdct.ID_DonViQuiDoi,
					hdct.ID_LoHang,
					IIF(ID_CheckIn = cn.ID and hd.YeuCau='4', hdct.TonLuyKe_NhanChuyenHang, hdct.TonLuyKe) AS TonKho, 
    				IIF(ID_CheckIn = cn.ID and hd.YeuCau='4', hdct.GiaVon_NhanChuyenHang, hdct.GiaVon) AS GiaVon
    		FROM BH_HoaDon_ChiTiet hdct
    		JOIN BH_HoaDon hd ON hd.ID = hdct.ID_HoaDon
			JOIN @tblChiNhanh cn ON cn.ID = hd.ID_DonVi OR (hd.ID_CheckIn = cn.ID and hd.YeuCau = '4')		
    		where hd.ChoThanhToan = 0 
			AND hd.LoaiHoaDon = 10
    		and (hd.ID_CheckIn in (select ID from @tblChiNhanh)
			or (hd.ID_DonVi in (select ID from @tblChiNhanh )))
			and (hd.NgayLapHoaDon < @ToDate or hd.NgaySua < @ToDate)
		) tblUnion
		JOIN DonViQuiDoi dvqd ON tblUnion.ID_DonViQuiDoi = dvqd.ID	
		) tblRN where tblRN.RN = 1

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetAllBangLuong]
    @IDChiNhanhs [nvarchar](max),
    @TxtSearch [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @TrangThais [nvarchar](10),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    
   DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TxtSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	DECLARE @tblChiNhanh TABLE (ID uniqueidentifier)
    	insert into @tblChiNhanh
    	select Name from dbo.splitstring(@IDChiNhanhs);
    
    	with data_cte
    	as (
    	select bl.*, 		
			soquy.DaTra,
			soquy.LuongThucNhan,
			soquy.TruTamUngLuong,
			soquy.ThanhToan,
			soquy.LuongThucNhan - soquy.DaTra as ConLai
    	from NS_BangLuong bl
		
    	join (select ct.ID_BangLuong,
    				sum(ct.LuongThucNhan) as LuongThucNhan,
    				sum(isnull(soquy.TienThu,0)) as ThanhToan,
					sum(isnull(soquy.TruTamUngLuong,0)) as TruTamUngLuong,
					sum(isnull(soquy.TienThu,0)) +	sum(isnull(soquy.TruTamUngLuong,0)) as DaTra
    			from NS_BangLuong_ChiTiet ct
    			left join( select qct.ID_BangLuongChiTiet , 
						sum(qct.TienThu) as TienThu,
						sum(ISNULL(qct.TruTamUngLuong,0)) as TruTamUngLuong
    						from Quy_HoaDon_ChiTiet qct  
    						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID where qhd.TrangThai= 1 
    						group by  qct.ID_BangLuongChiTiet) soquy on ct.ID= soquy.ID_BangLuongChiTiet					
    			group by ct.ID_BangLuong
    			) soquy on bl.ID = soquy.ID_BangLuong
    	where exists (select Name from dbo.splitstring(@TrangThais) tt where bl.TrangThai = tt.Name)
    	and exists (select ID from @tblChiNhanh dv where bl.ID_DonVi= dv.ID)
    	and ((bl.TuNgay >= @FromDate and (bl.TuNgay <= @ToDate or bl.DenNgay <= @ToDate))
    		or ( bl.DenNgay <= @ToDate and ( bl.DenNgay >= @FromDate or bl.TuNgay >= @FromDate))
    			)
    	AND ((select count(Name) from @tblSearchString b where 
    					bl.MaBangLuong like '%'+b.Name+'%' 
    					or bl.TenBangLuong like '%'+b.Name+'%' 
    						or bl.GhiChu like '%'+b.Name+'%' 
    				
    						)=@count or @count=0)	
    	),
    	count_cte
    	as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
    				sum(LuongThucNhan) as TongPhaiTra,		
					sum(TruTamUngLuong) as TongTamUng,
					sum(ThanhToan) as TongThanhToan,
    				sum(DaTra) as TongDaTra,
    				sum(ConLai) as TongConLai
    			from data_cte
    		)
    		select dt.*, cte.*, ISNULL(nv.TenNhanVien,'') as NguoiDuyet,
				dv.SoDienThoai AS DienThoaiChiNhanh,
				dv.DiaChi AS DiaChiChiNhanh
    		from data_cte dt
			join DM_DonVi dv on dt.ID_DonVi= dv.ID
    		left join NS_NhanVien nv on dt.ID_NhanVienDuyet = nv.ID
    		cross join count_cte cte
    		order by dt.DenNgay desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetChiTietHD_MultipleHoaDon]
    @lstID_HoaDon [nvarchar](max)
AS
BEGIN
    SELECT 
    			cthd.ID,
    			cthd.ID_HoaDon,
    			cthd.ID_DonViQuiDoi,
    			cthd.ID_LoHang,
    			dvqd.ID_HangHoa,			
    			cthd.DonGia,
    			cthd.GiaVon,
    			cthd.SoLuong,
    			cthd.ThanhTien,
    			cthd.ThanhToan,
    			cthd.TienChietKhau AS GiamGia,
    			cthd.PTChietKhau,
    			cthd.ThoiGian,
    			cthd.GhiChu,
    			iif(cthd.TenHangHoaThayThe is null or cthd.TenHangHoaThayThe ='', hh.TenHangHoa, cthd.TenHangHoaThayThe) as TenHangHoaThayThe,
    			iif(cthd.TenHangHoaThayThe is null or cthd.TenHangHoaThayThe ='', hh.TenHangHoa, cthd.TenHangHoaThayThe) as TenHangHoa,
    			isnull(cthd.PTThue,0) as PTThue,
    			isnull(cthd.TienThue,0) as TienThue,
    			isnull(cthd.ThanhToan,0) as ThanhToan,
    			isnull(cthd.DonGiaBaoHiem,0) as DonGiaBaoHiem,
    			(cthd.DonGia - cthd.TienChietKhau) as GiaBan,
    			CAST(SoThuTu AS float) AS SoThuTu,
    			cthd.ID_KhuyenMai,
    			dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    			hh.LaHangHoa,
    			hh.QuanLyTheoLoHang,	
				hh.ID_Xe,
    			dvqd.TenDonViTinh,
    			dvqd.MaHangHoa,
    			dvqd.TyLeChuyenDoi,
    			dvqd.ThuocTinhGiaTri, 				
    			hh.ID_NhomHang as ID_NhomHangHoa,
    			ISNULL(MaLoHang,'') as MaLoHang  ,
    			lo.NgaySanXuat, 
    			lo.NgayHetHan,
    			hd.YeuCau,    
    			ISNULL(nhh.TenNhomHangHoa,'') as TenNhomHangHoa,
    			ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
    			ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
    			iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
    			concat(TenHangHoa ,    	
    		Case when dvqd.TenDonVitinh = '' or dvqd.TenDonViTinh is null then '' else ' (' + dvqd.TenDonViTinh + ')' end +
    		Case when lo.MaLoHang is null then '' else '. Lô: ' + lo.MaLoHang end) as TenHangHoaFull   				    									
    	FROM BH_HoaDon hd
    	JOIN BH_HoaDon_ChiTiet cthd ON hd.ID= cthd.ID_HoaDon
    	JOIN DonViQuiDoi dvqd ON cthd.ID_DonViQuiDoi = dvqd.ID
    	JOIN DM_HangHoa hh ON dvqd.ID_HangHoa= hh.ID
    		LEFT JOIN DM_LoHang lo ON cthd.ID_LoHang = lo.ID
    		left JOIN DM_NhomHangHoa nhh ON hh.ID_NhomHang= nhh.ID    		
    	WHERE cthd.ID_HoaDon in (Select * from splitstring(@lstID_HoaDon))  
    		and (cthd.ID_ChiTietDinhLuong= cthd.ID OR cthd.ID_ChiTietDinhLuong is null)
    			and (cthd.ID_ParentCombo= cthd.ID OR cthd.ID_ParentCombo is null)
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetChiTietHoaDon_afterTraHang]
    @ID_HoaDon [nvarchar](max)
AS
BEGIN
    set nocount on
    
    	---- get cthdmua
    	select ID		
    	into #temCTMua
    	from BH_HoaDon_ChiTiet ctm where ctm.ID_HoaDon= @ID_HoaDon
    
    	---- get cttra or ctsudung
    	select 
    		ct.ID_ChiTietGoiDV,
    		SUM(ct.SoLuong) as SoLuongTra
    	into #tmpHDTra
    	from BH_HoaDon_ChiTiet ct 
    	join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    	where hd.ChoThanhToan='0' and hd.LoaiHoaDon not in (8,2)
    	and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
    	and exists (select ctm.ID from #temCTMua ctm where ct.ID_ChiTietGoiDV= ctm.ID)
    	group by ct.ID_ChiTietGoiDV
    
    
    	---- get soluong xuatkho of hdsc
    		select SUM(ctxk.SoLuong) as SoLuongXuat, ctxk.ID_ChiTietGoiDV
    		into #ctxk
    		from BH_HoaDon_ChiTiet ctxk 
    		join BH_HoaDon hdxk on ctxk.ID_HoaDon = hdxk.ID
    		where hdxk.ID_HoaDon = @ID_HoaDon
    		and hdxk.LoaiHoaDon = 8 and hdxk.ChoThanhToan='0'		
    		group by ctxk.ID_ChiTietGoiDV			
    			
    
    
    select  distinct
    		CAST(ctm.SoThuTu as float) as SoThuTu,
    		ctm.ID, 
    		ctm.ID_DonViQuiDoi,
    		ctm.ID_LoHang,
    		ctm.ID_TangKem, 
    		ctm.TangKem, 
    		ctm.ID_ParentCombo,
    		ctm.ID_ChiTietDinhLuong,
    		ctm.SoLuong,
    		ISNULL(ctt.SoLuongTra,0) as SoLuongTra,
    		iif(hd.LoaiHoaDon =25 and hh.LaHangHoa ='1', 
    		isnull(xk.SoLuongXuat,0) - ISNULL(ctt.SoLuongTra,0) ,ctm.SoLuong - ISNULL(ctt.SoLuongTra,0)) as SoLuongConLai,
    		ctm.DonGia, isnull(gv.GiaVon,0) as GiaVon, ctm.ThanhTien, ctm.ThanhToan, 
    		ctm.TienChietKhau, 
    		ctm.TienChietKhau as GiamGia,
    		ctm.ThoiGian, ctm.GhiChu, ctm.PTChietKhau,
    		ctm.ID_HoaDon, ctm.ID_ViTri,
    		ctm.ID_LichBaoDuong,
    		isnull(nhh.TenNhomHangHoa,'') as TenNhomHangHoa,
    		CAST(ISNULL(ctm.TienThue,0) as float) as TienThue,CAST(ISNULL(ctm.PTThue,0) as float) as PTThue, 
    		CAST(ISNULL(ctm.ThoiGianBaoHanh,0) as float) as ThoiGianBaoHanh,
    		CAST(ISNULL(ctm.LoaiThoiGianBH,0) as float) as LoaiThoiGianBH,
    		Case when hh.LaHangHoa='1' then 0 else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end as PhiDichVu,
    			Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end as LaPTPhiDichVu,
    		lo.NgaySanXuat, lo.NgayHetHan, isnull(lo.MaLoHang,'') as MaLoHang, isnull(tk.TonKho,0) as TonKho,
    		hh.QuanLyTheoLoHang,
    		hh.LaHangHoa,
    		iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
    		hd.MaHoaDon,
    		hh.DichVuTheoGio,
    		hh.DuocTichDiem,
    		hh.SoPhutThucHien,
			isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
    		isnull(hh.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT,
    		qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		qd.TenDonViTinh,
			qd.ID_HangHoa,
			qd.MaHangHoa,
    		ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan,
    		CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
    		hh.LaHangHoa, 
    		hh.TenHangHoa, 
    		CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach, 
    		hh.ID_NhomHang as ID_NhomHangHoa, 
    		ISNULL(hh.GhiChu,'') as GhiChuHH,
    		ISNULL(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
    		isnull(ctm.DonGiaBaoHiem,0) as DonGiaBaoHiem,
    		iif(ctm.TenHangHoaThayThe is null or ctm.TenHangHoaThayThe ='', hh.TenHangHoa, ctm.TenHangHoaThayThe) as TenHangHoaThayThe		
    
    	from BH_HoaDon_ChiTiet ctm
    	join BH_HoaDon hd on ctm.ID_HoaDon= hd.ID
    	join DonViQuiDoi qd on ctm.ID_DonViQuiDoi = qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID 
    	join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID 
    	LEFT JOIN DM_LoHang lo ON ctm.ID_LoHang = lo.ID
    	left join DM_HangHoa_TonKho tk on (ctm.ID_DonViQuiDoi = tk.ID_DonViQuyDoi and (ctm.ID_LoHang = tk.ID_LoHang or ctm.ID_LoHang is null) and  tk.ID_DonVi = hd.ID_DonVi)
    	left join DM_GiaVon gv on (tk.ID_DonViQuyDoi = gv.ID_DonViQuiDoi and (ctm.ID_LoHang = gv.ID_LoHang or ctm.ID_LoHang is null) and gv.ID_DonVi = hd.ID_DonVi) 
    	left join  #tmpHDTra ctt  on ctm.ID = ctt.ID_ChiTietGoiDV 
    	left join #ctxk xk on ctm.ID = xk.ID_ChiTietGoiDV
    	where ctm.ID_HoaDon= @ID_HoaDon
    	and (ctm.ID_ChiTietDinhLuong is null or ctm.ID_ChiTietDinhLuong = ctm.ID)
    	and (ctm.ID_ParentCombo is null or ctm.ID_ParentCombo = ctm.ID)
    	and (hh.LaHangHoa = 0 or (hh.LaHangHoa = 1 and tk.TonKho is not null))
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetChiTietHoaDon_ByIDHoaDon]
    @ID_HoaDon [uniqueidentifier]	
AS
BEGIN
  set nocount on;

  declare @ID_DonVi uniqueidentifier, @loaiHD int, @ID_HoaDonGoc uniqueidentifier		
	select top 1 @ID_DonVi= ID_DonVi, @ID_HoaDonGoc= ID_HoaDon, @loaiHD= LoaiHoaDon from BH_HoaDon where ID= @ID_HoaDon

  if @loaiHD= 8
		begin
		select 
			ctxk.ID,ctxk.ID_DonViQuiDoi,ctxk.ID_LoHang,
			ctxk.SoLuong,
			ctxk.SoLuong as SoLuongXuatHuy,
			ctxk.DonGia,
			ctxk.GiaVon, 
			ctxk.GiaTriHuy as ThanhTien, 
			ctxk.GiaTriHuy as ThanhToan, 
			ctxk.TienChietKhau as GiamGia,
			ctxk.GhiChu,
			cast(ctxk.SoThuTu as float) as SoThuTu,
			hd.MaHoaDon,
			hd.NgayLapHoaDon,
			hd.ID_NhanVien,
    		nv.TenNhanVien,
			lh.NgaySanXuat,
    		lh.NgayHetHan,    			
    		dvqd.MaHangHoa,
    		hh.TenHangHoa,
			hh.TenHangHoa as TenHangHoaThayThe,
			Case when hh.QuanLyTheoLoHang is null then 'false' else hh.QuanLyTheoLoHang end as QuanLyTheoLoHang, 
    		concat(hh.TenHangHoa , '', dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
    		dvqd.TenDonViTinh as TenDonViTinh,
    		dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		Case when lh.MaLoHang is null then '' else lh.MaLoHang end as TenLoHang,
    		CAST(ROUND(3, 0) as float) as TrangThaiMoPhieu,
			ROUND(ISNULL(TonKho,0),2) as TonKho
		from 
		(
		--- get ct if has tpdinhluong
		select max(ct.ID) as ID,
			max(ct.SoThuTu) as SoThuTu,
			ct.ID_DonViQuiDoi,
			ct.ID_LoHang,
			@ID_HoaDon as ID_HoaDon,
			sum(ct.SoLuong) as SoLuong, 
			max(ct.DonGia) as DonGia,
			max(ct.DonGia) as GiaVon,
			sum(ct.SoLuong * ct.DonGia) as GiaTriHuy,			
			max(ct.TienChietKhau) as TienChietKhau,
			max(ct.GhiChu) as GhiChu
		from BH_HoaDon_ChiTiet ct
		where ct.ID_HoaDon= @ID_HoaDon		
		and (ct.ChatLieu is null or ct.ChatLieu!='5')
		group by ct.ID_DonViQuiDoi, ct.ID_LoHang		
		)ctxk
		join BH_HoaDon hd on hd.ID= ctxk.ID_HoaDon
		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
		join DonViQuiDoi dvqd on ctxk.ID_DonViQuiDoi = dvqd.ID
		join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
		left join DM_LoHang lh on ctxk.ID_LoHang = lh.ID
		left join DM_HangHoa_TonKho tk on (dvqd.ID = tk.ID_DonViQuyDoi and (lh.ID = tk.ID_LoHang or lh.ID is null) and  tk.ID_DonVi = @ID_DonVi)
		where (hh.LaHangHoa = 1 and tk.TonKho is not null)
		end
	else
	begin
		if @loaiHD in (1,3,2,25)
		begin
			select ctsd.ID_ChiTietGoiDV, sum(SoLuong) as SoLuongSuDung
			into #tblSDDV 
			from BH_HoaDon_ChiTiet ctsd
			join BH_HoaDon hd on ctsd.ID_HoaDon= hd.ID
			where exists (select ID from BH_HoaDon_ChiTiet ct where ct.ID_HoaDon= @ID_HoaDon and ct.ID_ChiTietGoiDV =  ctsd.ID_ChiTietGoiDV)
			and hd.ChoThanhToan= 0
			AND (ctsd.ID_ChiTietDinhLuong IS NULL OR ctsd.ID_ChiTietDinhLuong = ctsd.ID) --- khong get tpdinhluong khi sudung GDV
			group by ctsd.ID_ChiTietGoiDV

					select DISTINCT tbl.*, 
					isnull(hdXK.SoLuongXuat,0) as SoLuongXuat,
					isnull(hdmua.SoLuongMua,0) as SoLuongMua,
					isnull(hdmua.SoLuongMua,0) - isnull(hdmua.SoLuongDVDaSuDung,0) as SoLuongDVConLai,
					isnull(hdmua.SoLuongDVDaSuDung,0) as SoLuongDVDaSuDung
					FROM 
						 (SELECT
    							cthd.ID,cthd.ID_HoaDon,DonGia,cthd.GiaVon,SoLuong,ThanhTien,ThanhToan,cthd.ID_DonViQuiDoi, cthd.ID_ChiTietDinhLuong, cthd.ID_ChiTietGoiDV,
    							cthd.TienChietKhau AS GiamGia,PTChietKhau,cthd.GhiChu,cthd.TienChietKhau,
    							(cthd.DonGia - cthd.TienChietKhau) as GiaBan,
								qd.GiaBan as GiaBanHH, ---- used to nhaphang from hoadon
    							CAST(SoThuTu AS float) AS SoThuTu,cthd.ID_KhuyenMai, ISNULL(cthd.TangKem,'0') as TangKem, cthd.ID_TangKem,
								-- replace char enter --> char space
    							(REPLACE(REPLACE(TenHangHoa,CHAR(13),''),CHAR(10),'') +
    							CASE WHEN (qd.ThuocTinhGiaTri is null or qd.ThuocTinhGiaTri = '') then '' else '_' + qd.ThuocTinhGiaTri end +
    							CASE WHEN TenDonVitinh = '' or TenDonViTinh is null then '' else ' (' + TenDonViTinh + ')' end +
    							CASE WHEN MaLoHang is null then '' else '. Lô: ' + MaLoHang end) as TenHangHoaFull,
    				
    							hh.ID AS ID_HangHoa,
								hh.ID_Xe,
								hh.LaHangHoa,
								hh.QuanLyTheoLoHang,
								hh.TenHangHoa, 
								isnull(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
								ISNULL(nhh.TenNhomHangHoa,'') as TenNhomHangHoa,
								ISNULL(ID_NhomHang,'00000000-0000-0000-0000-000000000000') as ID_NhomHangHoa,	
    							TenDonViTinh,MaHangHoa,YeuCau,
    							lo.ID AS ID_LoHang,
								ISNULL(MaLoHang,'') as MaLoHang,
								lo.NgaySanXuat, lo.NgayHetHan,
								qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
								ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, 
								CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
								CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach,
								CAST(ISNULL(cthd.PTThue,0) as float) as PTThue,
								CAST(ISNULL(cthd.TienThue,0) as float) as TienThue,
								CAST(ISNULL(cthd.ThoiGianBaoHanh,0) as float) as ThoiGianBaoHanh,
								CAST(ISNULL(cthd.LoaiThoiGianBH,0) as float) as LoaiThoiGianBH,
								Case when hh.LaHangHoa='1' then 0 else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end PhiDichVu,
								Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end LaPTPhiDichVu,
								CAST(0 as float) as TongPhiDichVu, -- set default PhiDichVu = 0 (caculator again .js)
								CAST(ISNULL(cthd.Bep_SoLuongYeuCau,0) as float) as Bep_SoLuongYeuCau,
								CAST(ISNULL(cthd.Bep_SoLuongHoanThanh,0) as float) as Bep_SoLuongHoanThanh, -- view in CTHD NhaHang
								CAST(ISNULL(cthd.Bep_SoLuongChoCungUng,0) as float) as Bep_SoLuongChoCungUng,
								ISNULL(hh.SoPhutThucHien,0) as SoPhutThucHien, -- lay so phut theo cai dat
								ISNULL(cthd.ThoiGianThucHien,0)  as ThoiGianThucHien,-- sophut thuc te thuchien	
								ISNULL(cthd.QuaThoiGian,0)  as QuaThoiGian,
				
								case when hh.LaHangHoa='0' then 0 else ISNULL(tk.TonKho,0) end as TonKho,
								cthd.ID_ViTri,
								ISNULL(vt.TenViTri,'') as TenViTri,			
								ThoiGian,cthd.ThoiGianHoanThanh, ISNULL(hh.GhiChu,'') as GhiChuHH,
								ISNULL(cthd.DiemKhuyenMai,0) as DiemKhuyenMai,
								ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
								ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
								ISNULL(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
								ISNULL(hh.ChietKhauMD_NVTheoPT,'0') as ChietKhauMD_NVTheoPT,
								cthd.ChatLieu,
								isnull(cthd.DonGiaBaoHiem,0) as DonGiaBaoHiem,
								iif(cthd.TenHangHoaThayThe is null or cthd.TenHangHoaThayThe ='',hh.TenHangHoa, cthd.TenHangHoaThayThe) as TenHangHoaThayThe,					
								cthd.ID_LichBaoDuong,
								iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
								cthd.ID_ParentCombo,
								qd.GiaNhap
					
    					FROM BH_HoaDon hd
    					JOIN BH_HoaDon_ChiTiet cthd ON hd.ID= cthd.ID_HoaDon
    					JOIN DonViQuiDoi qd ON cthd.ID_DonViQuiDoi = qd.ID
    					JOIN DM_HangHoa hh ON qd.ID_HangHoa= hh.ID    		
    					left JOIN DM_NhomHangHoa nhh ON hh.ID_NhomHang= nhh.ID    							
    					LEFT JOIN DM_LoHang lo ON cthd.ID_LoHang = lo.ID
						left join DM_HangHoa_TonKho tk on cthd.ID_DonViQuiDoi= tk.ID_DonViQuyDoi and tk.ID_DonVi= @ID_DonVi
						left join DM_ViTri vt on cthd.ID_ViTri= vt.ID
    					-- chi get CT khong phai la TP dinh luong
    					WHERE cthd.ID_HoaDon = @ID_HoaDon
								and (cthd.ChatLieu is null or cthd.ChatLieu!='5')
								AND (cthd.ID_ChiTietDinhLuong IS NULL OR cthd.ID_ChiTietDinhLuong = cthd.ID)
								and ((tk.ID_DonVi = hd.ID_DonVi and hh.LaHangHoa='1') 
								or tk.ID_DonVi is null
								or (hh.LaHangHoa='0'))
								and (cthd.ID_LoHang= tk.ID_LoHang OR (cthd.ID_LoHang is null and tk.ID_LoHang is null)) 
								and (cthd.ID_ParentCombo IS NULL OR cthd.ID_ParentCombo = cthd.ID) --- khong get tpcombo
						) tbl
						left join
						(
							select ctm.ID as ID_ChiTietGoiDV, ctm.SoLuong as SoLuongMua, isnull(ctsd.SoLuongSuDung,0) as SoLuongDVDaSuDung
							from BH_HoaDon_ChiTiet ctm
							join #tblSDDV ctsd  on ctm.ID= ctsd.ID_ChiTietGoiDV			
						) hdmua on tbl.ID_ChiTietGoiDV = hdmua.ID_ChiTietGoiDV
						left join 
						(
						--- soluongxuatkho
							select SUM(ctxk.SoLuong) as SoLuongXuat, ctxk.ID_ChiTietGoiDV
							from BH_HoaDon_ChiTiet ctxk 
							join BH_HoaDon hdxk on ctxk.ID_HoaDon = hdxk.ID
							where hdxk.ID_HoaDon = @ID_HoaDon
							and hdxk.LoaiHoaDon = 8 and hdxk.ChoThanhToan='0'
							group by ctxk.ID_ChiTietGoiDV			
						) hdXK on tbl.ID = hdXK.ID_ChiTietGoiDV 
						order by tbl.SoThuTu
		end
		else
			begin
				SELECT 
    			cthd.ID,
				cthd.ID_HoaDon, 
				cthd.ID_ParentCombo,
				cthd.ID_ChiTietDinhLuong,
				cthd.DonGia, 
				cthd.GiaVon, 
				cast(cthd.SoThuTu as float) as SoThuTu,
				SoLuong, 
				cthd.ThanhTien, 
				TienChietKhau, 
				cthd.ThanhToan, 
				cthd.TienThue, 
				isnull(cthd.PTThue,0) as PTThue, 
				dvqd.ID as ID_DonViQuiDoi,
    			dvqd.ID_HangHoa, dvqd.TenDonViTinh, dvqd.MaHangHoa,
				TienChietKhau as GiamGia, PTChietKhau,
				(cthd.DonGia - cthd.TienChietKhau) as GiaBan,
				hd.NgayLapHoaDon as ThoiGian, cthd.GhiChu,
    			cthd.ID_KhuyenMai,			
				lo.NgaySanXuat, lo.NgayHetHan,
    			dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				concat(TenHangHoa ,
    			dvqd.ThuocTinhGiaTri, 
    			Case when dvqd.TenDonVitinh = '' or dvqd.TenDonViTinh is null then '' else ' (' + dvqd.TenDonViTinh + ')' end +
    			Case when lo.MaLoHang is null then '' else '. Lô: ' + lo.MaLoHang end) as TenHangHoaFull,
    			LaHangHoa, QuanLyTheoLoHang,
				TenHangHoa,		
				hh.TenHangHoa as TenHangHoaThayThe,
    			TyLeChuyenDoi, YeuCau,
    			lo.ID AS ID_LoHang, ISNULL(MaLoHang,'') as MaLoHang, 			
				ISNULL(hhtonkho.TonKho, 0) as TonKho, 
				hd.ID_DonVi, dvqd.GiaNhap, 
				dvqd.GiaBan as GiaBanMaVach, hh.ID_NhomHang as ID_NhomHangHoa,
				dvqd.LaDonViChuan, hh.ChiPhiThucHien as PhiDichVu, cast(ISNULL(hh.QuyCach,1) as float) as QuyCach, 
				Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end as LaPTPhiDichVu,
				dvqd.GiaBan, dvqd.GiaBan as GiaBanHH, -- use to get banggiachung  of cthd (at NhapHangChiTiet),
				ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
				ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
				ISNULL(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
				ISNULL(hh.ChietKhauMD_NVTheoPT,'0') as ChietKhauMD_NVTheoPT,
				ISNULL(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
				cthd.ID_LichBaoDuong,
				iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa
    			FROM BH_HoaDon hd
    			JOIN BH_HoaDon_ChiTiet cthd ON hd.ID= cthd.ID_HoaDon
    			JOIN DonViQuiDoi dvqd ON cthd.ID_DonViQuiDoi = dvqd.ID
    			JOIN DM_HangHoa hh ON dvqd.ID_HangHoa= hh.ID
    			LEFT JOIN DM_LoHang lo ON cthd.ID_LoHang = lo.ID
				LEFT JOIN DM_HangHoa_TonKho hhtonkho on dvqd.ID = hhtonkho.ID_DonViQuyDoi
				and (hhtonkho.ID_LoHang = cthd.ID_LoHang or cthd.ID_LoHang is null) and hhtonkho.ID_DonVi = @ID_DonVi
    			WHERE cthd.ID_HoaDon = @ID_HoaDon 
				and (cthd.ID_ChiTietDinhLuong = cthd.ID or cthd.ID_ChiTietDinhLuong is null)
				and (cthd.ID_ParentCombo IS NULL OR cthd.ID_ParentCombo = cthd.ID)
				and (cthd.ChatLieu is null or cthd.ChatLieu!='5')
				order by cthd.SoThuTu desc
			end
	end
    END");

			Sql(@"ALTER PROCEDURE [dbo].[GetDSGoiDichVu_ofKhachHang]
    @IDChiNhanhs [nvarchar](50) = null,
	@IDCustomers [nvarchar](max) = null,
	@IDCars nvarchar(max) = null,
	@TextSearch nvarchar(max) = null,
	@DateFrom datetime = null,
	@DateTo datetime = null    
AS
BEGIN
    SET NOCOUNT ON;
	declare @sql nvarchar(max)='', @where nvarchar(max)='', @paramDefined nvarchar(max)=''
	declare @tbldefined nvarchar(max) =' declare @tblChiNhanh table(ID uniqueidentifier) 
								declare @tblCus table(ID uniqueidentifier)
								declare @tblCar table(ID uniqueidentifier)'

	set @where =' where 1 = 1 and hd.LoaiHoaDon = 19 
    			and hd.ChoThanhToan=0
				and ctm.ChatLieu != 5
				and (ctm.ID_ChiTietDinhLuong is null or ctm.ID= ctm.ID_ChiTietDinhLuong) '

	if isnull(@IDChiNhanhs,'')!=''
		begin
			set @sql = CONCAT(@sql, ' insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanh_In) ')
			set @where= CONCAT(@where, ' and exists (select ID from @tblChiNhanh cn where cn.ID = hd.ID_DonVi)')
		end

	if isnull(@IDCustomers,'')!=''
		begin			
			set @where = CONCAT(@where , ' and exists (select ID from @tblCus cus where hd.ID_DoiTuong = cus.ID)')
			set @sql = CONCAT(@sql, ' insert into @tblCus select name from dbo.splitstring(@IDCustomers_In) ;')
		end
	if isnull(@IDCars,'')!=''
		begin
			set @where = CONCAT(@where , ' and exists (select ID from @tblCar car where hd.ID_Xe = car.ID)')
			set @sql = CONCAT(@sql, ' insert into @tblCar select name from dbo.splitstring(@IDCars_In) ;')
		end

	if isnull(@TextSearch,'')!=''
		set @where= CONCAT(@where, ' and (hd.MaHoaDon like N''%'' + @TextSearch_In + ''%'' 
			or qd.MaHangHoa like N''%'' + @TextSearch_In + ''%''  or hh.TenHangHoa like N''%'' + @TextSearch_In + ''%''
			 or hh.TenHangHoa_KhongDau like N''%'' + @TextSearch_In + ''%'')    ')

	if isnull(@DateFrom,'')!=''
		set @where= CONCAT(@where, ' and (hd.HanSuDungGoiDV is null or hd.HanSuDungGoiDV >= @DateFrom_In)   ')

	if isnull(@DateTo,'')!=''
		set @where= CONCAT(@where, ' and (hd.HanSuDungGoiDV is not null and hd.HanSuDungGoiDV < @DateTo_In)   ')

	set @sql = concat(@tbldefined, @sql, '

    select  
    		hd.ID as ID_GoiDV, MaHoaDon, 
			convert(varchar,hd.NgayLapHoaDon, 103) as NgayLapHoaDon,
    		convert(varchar,hd.NgayApDungGoiDV, 103) as NgayApDungGoiDV,
    		convert(varchar,hd.HanSuDungGoiDV, 103) as HanSuDungGoiDV, 		
    		ctm.ID as ID_ChiTietGoiDV, ctm.ID_DonViQuiDoi, ctm.ID_LoHang, 
    		ISNULL(ctm.ID_TangKem, ''00000000-0000-0000-0000-000000000000'') as ID_TangKem, ISNULL(ctm.TangKem,0) as TangKem, 
			ctm.TienChietKhau ,
			ctm.PTChietKhau ,
    		ctm.DonGia  as GiaBan,
    		ctm.SoLuong, 
    		ctm.SoLuong - ISNULL(ctt.SoLuongTra,0) as SoLuongMua,
    		ISNULL(ctt.SoLuongDung,0) as SoLuongDung,
    		round(ctm.SoLuong - ISNULL(ctt.SoLuongTra,0) - ISNULL(ctt.SoLuongDung,0),2) as SoLuongConLai,		
    		qd.TenDonViTinh,qd.ID_HangHoa,qd.MaHangHoa,ISNULL(qd.LaDonViChuan,0) as LaDonViChuan, CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
    		hh.LaHangHoa, 
			iif(ctm.TenHangHoaThayThe is null or ctm.TenHangHoaThayThe ='''', hh.TenHangHoa, ctm.TenHangHoaThayThe) as TenHangHoa,
			hh.TonToiThieu, CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach,
    		ISNULL(hh.ID_NhomHang,''00000000-0000-0000-0000-000000000001'') as ID_NhomHangHoa,
    		ISNULL(hh.SoPhutThucHien,0) as SoPhutThucHien,
    		case when hh.LaHangHoa = 1 then ''0'' else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end PhiDichVu,
    		Case when hh.LaHangHoa=1 then ''0'' else ISNULL(hh.ChiPhiTinhTheoPT,''0'') end as LaPTPhiDichVu,
    		ISNULL(hh.GhiChu,'''') as GhiChuHH,
    		ISNULL(ctm.GhiChu,'''') as GhiChu,
    		isnull(hh.QuanLyTheoLoHang,''0'') as QuanLyTheoLoHang,
    		lo.MaLoHang, lo.NgaySanXuat, lo.NgayHetHan, xe.BienSo, hd.ID_Xe,			
			ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
			ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
			ISNULL(hh.ThoiGianBaoHanh,0) as ThoiGianBaoHanh,
			ISNULL(hh.LoaiBaoHanh,0) as LoaiBaoHanh,			
			ISNULL(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
			ISNULL(hh.ChietKhauMD_NVTheoPT,''1'') as ChietKhauMD_NVTheoPT,
			ISNULL(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
			hh.LaHangHoa,
			ctm.ID_ParentCombo,			
			iif(ctm.ID_ParentCombo = ctm.ID, 0,1) as SoThuTu,
			isnull(ctm.GiaVon,0) as GiaVon
    	from BH_HoaDon_ChiTiet ctm
    	join BH_HoaDon hd on ctm.ID_HoaDon = hd.ID
		left join Gara_DanhMucXe xe on hd.ID_Xe= xe.ID
    	join DonViQuiDoi qd on ctm.ID_DonViQuiDoi = qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
    	left join DM_LoHang lo on ctm.ID_LoHang = lo.ID
    	left join 
    	(
    		select a.ID_ChiTietGoiDV,
    			SUM(a.SoLuongTra) as SoLuongTra,
    			SUM(a.SoLuongDung) as SoLuongDung
    		from
    			(-- sum soluongtra
    			select ct.ID_ChiTietGoiDV,
    				SUM(ct.SoLuong) as SoLuongTra,
    				0 as SoLuongDung
    			from BH_HoaDon_ChiTiet ct 
    			join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    			where hd.ChoThanhToan= 0 and hd.LoaiHoaDon = 6
    			and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
    			group by ct.ID_ChiTietGoiDV
    
    			union all
    			-- sum soluong sudung
    			select ct.ID_ChiTietGoiDV,
    				0 as SoLuongDung,
    				SUM(ct.SoLuong) as SoLuongDung
    			from BH_HoaDon_ChiTiet ct 
    			join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    			where hd.ChoThanhToan=0 and hd.LoaiHoaDon in (1,25)
    			and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
    			group by ct.ID_ChiTietGoiDV
    			) a group by a.ID_ChiTietGoiDV
    	) ctt on ctm.ID = ctt.ID_ChiTietGoiDV ', @where, ' order by hd.NgayLapHoaDon desc')

		print @sql
    	
		set @paramDefined =' @IDChiNhanh_In nvarchar(max),
							@IDCustomers_In nvarchar(max),
							@IDCars_In nvarchar(max),
							@TextSearch_in nvarchar(max),
							@DateFrom_In nvarchar(max),
							@DateTo_in nvarchar(max)'
		
			exec sp_executesql @sql, @paramDefined,
							@IDChiNhanh_In = @IDChiNhanhs,
							@IDCustomers_In = @IDCustomers,
							@IDCars_in = @IDCars,
							@TextSearch_In = @TextSearch,
							@DateFrom_In = @DateFrom,
							@DateTo_in = @DateTo

END");

			Sql(@"ALTER PROC [dbo].[getList_ChietKhauNhanVienTongHop]
@ID_ChiNhanhs [nvarchar](max),
@ID_NhanVienLogin nvarchar(max),
@TextSearch [nvarchar](500),
@DateFrom [nvarchar](max),
@DateTo [nvarchar](max),
@CurrentPage int,
@PageSize int
AS
BEGIN
set nocount on;
	
	declare @tab_DoanhSo TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), TongDoanhThu float, TongThucThu float, HoaHongDoanhThu float, HoaHongThucThu float, TongAll float,
	Status_DoanhThu varchar(4),	TotalRow int, TotalPage float, TongAllDoanhThu float, TongAllThucThu float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongAllAll float)
	INSERT INTO @tab_DoanhSo exec GetAll_DiscountSale @ID_ChiNhanhs,@ID_NhanVienLogin,  @DateFrom, @DateTo, @TextSearch, '', 0,500;	

	DECLARE @tab_HoaDon TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float,
	TotalRow int, TotalPage float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongHoaHongVND float, TongAllAll float)
	INSERT INTO @tab_HoaDon exec ReportDiscountInvoice @ID_ChiNhanhs,@ID_NhanVienLogin,  @TextSearch,'0,1,6,19,22,25', @DateFrom, @DateTo, 8, 1, 0, 0,500;

	DECLARE @tab_HangHoa TABLE (MaNhanVien nvarchar(255), TenNhanVien nvarchar(max),ID_NhanVien uniqueidentifier, HoaHongThucHien float, HoaHongThucHien_TheoYC float, HoaHongTuVan float, HoaHongBanGoiDV float, Tong float,
	TotalRow int, TotalPage float, TongHoaHongThucHien float, TongHoaHongThucHien_TheoYC float,  TongHoaHongTuVan float, TongHoaHongBanGoiDV float, TongAll float)
	INSERT INTO @tab_HangHoa exec ReportDiscountProduct_General @ID_ChiNhanhs,@ID_NhanVienLogin,  @TextSearch,'0,1,6,19,22,25', @DateFrom, @DateTo, 16,1, 0,500;	

	with data_cte
	as
	 (
		SELECT a.ID_NhanVien, a.MaNhanVien, a.TenNhanVien, 
			SUM(HoaHongThucHien) as HoaHongThucHien,
			SUM(HoaHongThucHien_TheoYC) as HoaHongThucHien_TheoYC,
			SUM(HoaHongTuVan) as HoaHongTuVan,
			SUM(HoaHongBanGoiDV) as HoaHongBanGoiDV,
			SUM(TongHangHoa) as TongHangHoa,
			SUM(HoaHongDoanhThuHD) as HoaHongDoanhThuHD,
			SUM(HoaHongThucThuHD) as HoaHongThucThuHD,
			SUM(HoaHongVND) as HoaHongVND,
			SUM(TongHoaDon) as TongHoaDon,
			SUM(DoanhThu) as DoanhThu,
			SUM(ThucThu) as ThucThu,
			SUM(HoaHongDoanhThuDS) as HoaHongDoanhThuDS,
			SUM(HoaHongThucThuDS) as HoaHongThucThuDS,
			SUM(TongDoanhSo) as TongDoanhSo,
			SUM(TongDoanhSo + TongHoaDon + TongHangHoa) as Tong
		FROM 
		(
		select ID_NhanVien, MaNhanVien, TenNhanVien, 
		HoaHongThucHien, 
		HoaHongThucHien_TheoYC, 
		HoaHongTuVan, 
		HoaHongBanGoiDV, 
		Tong as TongHangHoa,
		0 as HoaHongDoanhThuHD,
		0 as HoaHongThucThuHD,
		0 as HoaHongVND,
		0 as TongHoaDon,
		0 as DoanhThu,
		0 as ThucThu,
		0 as HoaHongDoanhThuDS,
		0 as HoaHongThucThuDS,
		0 as TongDoanhSo
		from @tab_HangHoa
		UNION ALL
		Select ID_NhanVien, MaNhanVien, TenNhanVien, 
		0 as HoaHongThucHien,
		0 as HoaHongThucHien_TheoYC,
		0 as HoaHongTuVan,
		0 as HoaHongBanGoiDV,
		0 as TongHangHoa,
		HoaHongDoanhThu as HoaHongDoanhThuHD,
		HoaHongThucThu as HoaHongThucThuHD,
		HoaHongVND,
		TongAll as TongHoaDon,
		0 as DoanhThu,
		0 as ThucThu,
		0 as HoaHongDoanhThuDS,
		0 as HoaHongThucThuDS,
		0 as TongDoanhSo
		from @tab_HoaDon
		UNION ALL
		Select ID_NhanVien, MaNhanVien, TenNhanVien, 
		0 as HoaHongThucHien,
		0 as HoaHongThucHien_TheoYC,
		0 as HoaHongTuVan,
		0 as HoaHongBanGoiDV,
		0 as TongHangHoa,
		0 as HoaHongDoanhThuHD,
		0 as HoaHongThucThuHD,
		0 as HoaHongVND,
		0 as TongHoaDon,
		TongDoanhThu as DoanhThu,
		TongThucThu as ThucThu,
		HoaHongDoanhThu as HoaHongDoanhThuDS,
		HoaHongThucThu as HoaHongThucThuDS,
		TongAll as TongDoanhSo
		from @tab_DoanhSo
		) as a
		GROUP BY a.ID_NhanVien, a.MaNhanVien, a.TenNhanVien
	),
	count_cte
	as (
		select count(ID_NhanVien) as TotalRow,
			CEILING(COUNT(ID_NhanVien) / CAST(@PageSize as float ))  as TotalPage,
			sum(HoaHongThucHien) as TongHoaHongThucHien,
			sum(HoaHongThucHien_TheoYC) as TongHoaHongThucHien_TheoYC,
			sum(HoaHongTuVan) as TongHoaHongTuVan,
			sum(HoaHongBanGoiDV) as TongHoaHongBanGoiDV,
			sum(TongHangHoa) as TongHoaHong_TheoHangHoa,

			sum(HoaHongDoanhThuHD) as TongHoaHongDoanhThu,
			sum(HoaHongThucThuHD) as TongHoaHongThucThu,
			sum(HoaHongVND) as TongHoaHongVND,
			sum(TongHoaDon) as TongHoaHong_TheoHoaDon,
			sum(DoanhThu) as TongDoanhThu,
			sum(ThucThu) as TongThucThu,

			sum(HoaHongDoanhThuDS) as TongHoaHongDoanhThuDS,
			sum(HoaHongThucThuDS) as TongHoaHongThucThuDS,
			sum(TongDoanhSo) as TongHoaHong_TheoDoanhSo,
			sum(Tong) as TongHoaHongAll

		from data_cte
		)
		select dt.*, cte.*
		from data_cte dt
		cross join count_cte cte
		order by dt.MaNhanVien
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetList_GoiDichVu_afterUseAndTra]
   @IDChiNhanhs nvarchar(max) = null,
   @IDNhanViens nvarchar(max) = null,
   @DateFrom datetime = null,
   @DateTo datetime = null,
   @TextSearch nvarchar(max) = null,
   @CurrentPage int =null,
   @PageSize int = null
AS
BEGIN

	set nocount on;

	if isnull(@CurrentPage,'') =''
		set @CurrentPage = 0
	if isnull(@PageSize,'') =''
		set @PageSize = 10


	DECLARE @tblNhanVien TABLE (ID UNIQUEIDENTIFIER, TenNhanVien NVARCHAR(MAX));
	IF(ISNULL(@IDNhanViens, '') != '' )
	BEGIN
		INSERT INTO @tblNhanVien
		SELECT nv.ID, nv.TenNhanVien FROM NS_NhanVien nv
		INNER JOIN (SELECT name from dbo.splitstring(@IDNhanViens)) a ON a.Name = nv.ID;
	END
	ELSE
	BEGIN
		INSERT INTO @tblNhanVien
		SELECT ID, TenNhanVien FROM NS_NhanVien;
	END

	DECLARE @tblSearch TABLE (Name [nvarchar](max))
	DECLARE @count int
	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!=''
	select @count =  (Select count(*) from @tblSearch)

	DECLARE @VDateFrom DATETIME;
	IF(ISNULL(@DateFrom, '') != '')
	BEGIN
		SET @VDateFrom = @DateFrom;
	END
	ELSE
	BEGIN
		SET @VDateFrom = '1999-01-01';
	END
	DECLARE @VDateTo DATETIME;
	IF(ISNULL(@DateTo, '') != '')
	BEGIN
		SET @VDateTo = @DateTo;
	END
	ELSE
	BEGIN
		SET @VDateTo = DATEADD(DAY, 10, GETDATE());
	END

	DECLARE @tblHoaDonGoiDichVu TABLE(ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), ID_HoaDon UNIQUEIDENTIFIER, ID_BangGia UNIQUEIDENTIFIER,
	ID_NhanVien UNIQUEIDENTIFIER, ID_DonVi UNIQUEIDENTIFIER, NguoiTao NVARCHAR(MAX), ID_Xe UNIQUEIDENTIFIER, DienGiai NVARCHAR(MAX), NgayLapHoaDon DATETIME, TenNhanVien NVARCHAR(MAX), 
	ID_DoiTuong UNIQUEIDENTIFIER, TenDoiTuong NVARCHAR(MAX), MaDoiTuong NVARCHAR(MAX), BienSo NVARCHAR(MAX), PhaiThanhToan FLOAT, TongTienHang FLOAT, TongGiamGia FLOAT, DiemGiaoDich FLOAT,
	LoaiHoaDon INT, TongTienThue FLOAT, TongThueKhachHang FLOAT)

	INSERT INTO @tblHoaDonGoiDichVu
	SELECT 
	hd.ID, 
	hd.MaHoaDon,
	hd.ID_HoaDon,
	hd.ID_BangGia,
	hd.ID_NhanVien,
	hd.ID_DonVi,
	hd.NguoiTao,
	hd.ID_Xe,
	hd.DienGiai,
	hd.NgayLapHoaDon,
	ISNULL(nv.TenNhanVien,'' ) as TenNhanVien,
	hd.ID_DoiTuong,
	ISNULL(dt.TenDoiTuong,N'Khách lẻ' ) as TenDoiTuong,
	ISNULL(dt.MaDoiTuong,'kl' ) as MaDoiTuong,
	xe.BienSo,
	ISNULL(hd.PhaiThanhToan, 0) as PhaiThanhToan,
	ISNULL(hd.TongTienHang, 0) as TongTienHang,
	ISNULL(hd.TongGiamGia, 0) as TongGiamGia,
	ISNULL(hd.DiemGiaoDich, 0) as DiemGiaoDich,
	hd.LoaiHoaDon,
	ISNULL(hd.TongTienThue, 0) as TongTienThue ,
	ISNULL(hd.TongThueKhachHang, 0) as TongThueKhachHang
	--isnull(thuchi.TienThu,0) as KhachDaTra,
	--hd.PhaiThanhToan - isnull(thuchi.TienThu,0) as ConNo
	FROM BH_HoaDon hd
	INNER JOIN (SELECT name FROM dbo.splitstring(ISNULL(@IDChiNhanhs, ''))) dv ON dv.Name = hd.ID_DonVi
	LEFT JOIN @tblNhanVien nv ON hd.ID_NhanVien = nv.ID
	LEFT JOIN Gara_DanhMucXe xe ON xe.ID = hd.ID_Xe
	LEFT JOIN DM_DoiTuong dt ON hd.ID_DoiTuong = dt.ID
	--left join (
	--				select 
	--					tblQuy.ID_HoaDonLienQuan,
	--					sum(tblQuy.ThuTuThe) as ThuTuThe,
	--					sum(tblQuy.TienThu) as TienThu
	--				from
	--				(
	--					---- Thu tu HDMua
	--					select qct.ID_HoaDonLienQuan,
	--						case when qhd.TrangThai= 0 then 0 else SUM(iif(qct.HinhThucThanhToan= 4, qct.TienThu,0)) end as ThuTuThe,							
	--						Case when qhd.TrangThai = 0 then 0 else 
	--							Case when qhd.LoaiHoaDon = 11 then SUM(ISNULL(qct.Tienthu, 0)) else - SUM(ISNULL(qct.Tienthu, 0)) end end as TienThu
	--					from Quy_HoaDon_ChiTiet qct					
	--					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID												
	--				group by qct.ID_HoaDonLienQuan, qhd.TrangThai,qhd.LoaiHoaDon
	--				) tblQuy group by tblQuy.ID_HoaDonLienQuan
	--) thuchi on hd.ID= thuchi.ID_HoaDonLienQuan
	WHERE hd.LoaiHoaDon = 19 and hd.ChoThanhToan = 0 AND hd.NgayLapHoaDon BETWEEN @VDateFrom AND @VDateTo
	and 
	((select count(Name) from @tblSearch b where     			
	hd.MaHoaDon like '%'+b.Name+'%'
	or hd.NguoiTao like '%'+b.Name+'%'
	or xe.BienSo like '%'+b.Name+'%'	
	or dt.MaDoiTuong like '%'+b.Name+'%'		
	or dt.TenDoiTuong like '%'+b.Name+'%'
	or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
	or dt.DienThoai like '%'+b.Name+'%'		
	)=@count or @count=0)

	-- Hóa đơn mua
	DECLARE @tblTonGoiDichVuChiTiet TABLE(IDGoiDV UNIQUEIDENTIFIER, IDDonViQuiDoi UNIQUEIDENTIFIER, IDChiTietGoiDichVu UNIQUEIDENTIFIER, SoLuongTon FLOAT);
	INSERT INTO @tblTonGoiDichVuChiTiet
	select 
		hd.ID as ID_GoiDV,
		hdct.ID_DonViQuiDoi,
		hdct.ID,
		hdct.SoLuong
	from @tblHoaDonGoiDichVu hd
	inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
	where hdct.ID_ChiTietDinhLuong = hdct.ID  OR hdct.ID_ChiTietDinhLuong is null;

	UPDATE tdv
	SET tdv.SoLuongTon = tdv.SoLuongTon - hdsd.SoLuongSuDung
	FROM @tblTonGoiDichVuChiTiet tdv
	INNER JOIN
	(select ct2.ID_ChiTietGoiDV, SUM(ct2.SoLuong) AS SoLuongSuDung	
	from BH_HoaDon_ChiTiet ct2 
	join BH_HoaDon hd2 on ct2.ID_HoaDon =hd2.ID
	where hd2.ChoThanhToan= 0 and hd2.LoaiHoaDon = 1  and (ct2.ID_ChiTietDinhLuong is null or ct2.ID_ChiTietDinhLuong =ct2.ID)
	GROUP BY ct2.ID_ChiTietGoiDV
	) hdsd
	ON hdsd.ID_ChiTietGoiDV = tdv.IDChiTietGoiDichVu;

	DECLARE @tblTonGoiDichVu TABLE(IDGoiDV UNIQUEIDENTIFIER, SoLuongTon FLOAT);
	INSERT INTO @tblTonGoiDichVu
	SELECT IDGoiDV, SUM(SoLuongTon) FROM @tblTonGoiDichVuChiTiet
	GROUP BY IDGoiDV;
	-- Hóa đơn trả
	UPDATE tdv
	SET tdv.SoLuongTon = tdv.SoLuongTon - hdt.SoLuongTra
	FROM @tblTonGoiDichVu tdv
	INNER JOIN
	(select 
		hd3.ID_HoaDon as ID_HoaDonGoc, 
		SUM(ISNULL(ct3.SoLuong, 0)) AS SoLuongTra
	from BH_HoaDon_ChiTiet ct3
	INNER join BH_HoaDon hd3 on ct3.ID_HoaDon = hd3.ID 
	where hd3.ChoThanhToan =0 AND hd3.ID_HoaDon IS NOT NULL
	group by hd3.ID_HoaDon) hdt ON hdt.ID_HoaDonGoc = tdv.IDGoiDV

	select tblView.*,
		isnull(soquy.TienThu,0) as HDGoc_DaTra,
		tblView.PhaiThanhToan -  isnull(soquy.TienThu,0) as ConNoHDGoc,
		Isnull(hdt.PhaiThanhToan,0) as TongTra
	into #tblX
	from
	(
		SELECT R.ID, R.MaHoaDon, R.ID_HoaDon, R.ID_BangGia, R.ID_NhanVien, R.ID_DonVi, R.NguoiTao, R.ID_Xe,
		R.DienGiai, R.NgayLapHoaDon, R.TenNhanVien, R.ID_DoiTuong, R.TenDoiTuong, R.MaDoiTuong,
		R.BienSo, R.PhaiThanhToan, R.TongTienHang, R.TongGiamGia, R.DiemGiaoDich, R.LoaiHoaDon, R.TongTienThue, R.TongThueKhachHang,
		R.TotalRow, R.SoLuongTon AS SoLuongConLai FROM
		(SELECT row_number() over (order by gdv.NgayLapHoaDon desc) as Rn,
					COUNT(gdv.ID) OVER () as TotalRow, * FROM @tblHoaDonGoiDichVu gdv
		INNER JOIN @tblTonGoiDichVu tdv ON gdv.ID = tdv.IDGoiDV
		WHERE tdv.SoLuongTon > 0) R
		WHERE R.Rn BETWEEN (@CurrentPage * @PageSize) + 1 AND @PageSize * (@CurrentPage + 1)
	) tblView
	left join (
					select 
						tblQuy.ID_HoaDonLienQuan,
						sum(tblQuy.ThuTuThe) as ThuTuThe,
						sum(tblQuy.TienThu) as TienThu
					from
					(
						---- Thu tu HDMua
						select qct.ID_HoaDonLienQuan,
							case when qhd.TrangThai= 0 then 0 else SUM(iif(qct.HinhThucThanhToan= 4, qct.TienThu,0)) end as ThuTuThe,							
							Case when qhd.TrangThai = 0 then 0 else 
								Case when qhd.LoaiHoaDon = 11 then SUM(ISNULL(qct.Tienthu, 0)) else - SUM(ISNULL(qct.Tienthu, 0)) end end as TienThu
						from Quy_HoaDon_ChiTiet qct					
						join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID												
					group by qct.ID_HoaDonLienQuan, qhd.TrangThai,qhd.LoaiHoaDon
					) tblQuy group by tblQuy.ID_HoaDonLienQuan
	) soquy on tblView.ID = soquy.ID_HoaDonLienQuan
	left join BH_HoaDon hdt on tblView.ID= hdt.ID_HoaDon and hdt.ChoThanhToan='0'
	
	
	select tblView.*,
		isnull(tbl.TongGoc,0) as TongGoc,
		isnull(tbl.TongThuGoc,0) as TongThuGoc,
		isnull(tbl.TongTra,0) as TongTra,
		case when tblView.ID_HoaDon is null	
			then iif(tblView.TongTra >= tblView.ConNoHDGoc,0, tblView.ConNoHDGoc - tblView.TongTra)
		else tblView.ConNoHDGoc + isnull(tbl.TongGoc,0) -isnull(tbl.TongThuGoc,0) -isnull(tbl.TongTra,0) end as ConNo
	from #tblX tblView
	left join
	(
		----- get hdGoc + hdTra
		select hdTra.ID, tbl.ID_HoaDon,			
			ISNULL(hdGoc.PhaiThanhToan, 0) AS TongGoc,
			isnull(soquy.TienThu,0) as TongThuGoc,
			hdTra.PhaiThanhToan as TongTra
		from BH_HoaDon hdTra
		join #tblX tbl on hdTra.ID = tbl.ID_HoaDon
		left join BH_HoaDon hdGoc on hdTra.ID_HoaDon = hdGoc.ID
		left join (
						select 
							tblQuy.ID_HoaDonLienQuan,
							sum(tblQuy.ThuTuThe) as ThuTuThe,
							sum(tblQuy.TienThu) as TienThu
						from
						(
							---- Thu tu HDGoc
							select qct.ID_HoaDonLienQuan,
								case when qhd.TrangThai= 0 then 0 else SUM(iif(qct.HinhThucThanhToan= 4, qct.TienThu,0)) end as ThuTuThe,							
								Case when qhd.TrangThai = 0 then 0 else 
									Case when qhd.LoaiHoaDon = 11 then SUM(ISNULL(qct.Tienthu, 0)) else - SUM(ISNULL(qct.Tienthu, 0)) end end as TienThu
							from Quy_HoaDon_ChiTiet qct					
							join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID												
						group by qct.ID_HoaDonLienQuan, qhd.TrangThai,qhd.LoaiHoaDon
						) tblQuy group by tblQuy.ID_HoaDonLienQuan
		) soquy on hdGoc.ID = soquy.ID_HoaDonLienQuan		
	) tbl on  tblView.ID_HoaDon  = tbl.ID_HoaDon

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetList_GoiDichVu_Where]
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @maHD [nvarchar](max)
AS
BEGIN
	set nocount on;

	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@maHD, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);

	if @timeStart='2016-01-01'		
		select @timeStart = min(NgayLapHoaDon) from BH_HoaDon where LoaiHoaDon=19
    SELECT 
    	c.ID,
    	c.ID_BangGia,
    	c.ID_HoaDon,
    	c.ID_ViTri,
    	c.ID_NhanVien,
    	c.ID_DoiTuong,
		c.ID_Xe,
		xe.BienSo,
		c.ID_PhieuTiepNhan,
    	c.TheoDoi,
    	c.ID_DonVi,
    	c.ID_KhuyenMai,
    	c.ChoThanhToan,
    	c.MaHoaDon,
    	c.MaHoaDonGoc,
    	c.TongTienHDTra,
    	c.NgayLapHoaDon,
		ISNULL(c.MaDoiTuong,'') as MaDoiTuong,
    	c.TenDoiTuong,
    	c.Email,
    	c.DienThoai,
    	c.NguoiTaoHD,
    	c.DiaChiKhachHang,
    	c.KhuVuc,
    	c.PhuongXa,
    	c.TenDonVi,
    	c.TenNhanVien,
    	c.DienGiai,
    	c.TenBangGia,
    	c.TenPhongBan,
    	c.TongTienHang, c.TongGiamGia, 
		c.TongThanhToan,
		case when c.PhaiThanhToan < c.TongTienHDTra then 0 else  c.PhaiThanhToan - c.TongTienHDTra - c.KhachDaTra end as ConNo,
		case when c.PhaiThanhToan < c.TongTienHDTra then 0 else  c.PhaiThanhToan - c.TongTienHDTra end as PhaiThanhToan,
		c.ThuTuThe, c.TienMat, c.TienATM,c.TienDoiDiem, c.ChuyenKhoan, c.KhachDaTra,c.TongChietKhau,c.TongTienThue,PTThueHoaDon,
		c.TongThueKhachHang,
		ID_TaiKhoanPos,
		ID_TaiKhoanChuyenKhoan,
    	c.TrangThai,
    	c.KhuyenMai_GhiChu,
    	c.KhuyeMai_GiamGia,
    	c.LoaiHoaDonGoc,
    	c.LoaiHoaDon,
    	c.DiaChiChiNhanh,
    	c.DienThoaiChiNhanh,
    	c.DiemGiaoDich,
    	c.DiemSauGD, -- add 02.08.2018 (bind InHoaDon)
    	c.HoaDon_HangHoa, -- string contail all MaHangHoa,TenHangHoa of HoaDon
    	CONVERT(nvarchar(10),c.NgayApDungGoiDV,103) as NgayApDungGoiDV,
    	CONVERT(nvarchar(10),c.HanSuDungGoiDV,103) as HanSuDungGoiDV
    	FROM
    	(
    		select 
    		a.ID as ID,
    		hdXMLOut.HoaDon_HangHoa,
    		bhhd.ID_DoiTuong,
    			-- Neu theo doi = null --> kiem tra neu la khach le --> theodoi = true, nguoc lai = 1
    		CASE 
    			WHEN dt.TheoDoi IS NULL THEN 
    				CASE WHEN dt.ID IS NULL THEN '0' ELSE '1' END
    			ELSE dt.TheoDoi
    		END AS TheoDoi,
    		bhhd.ID_HoaDon,
    		bhhd.ID_NhanVien,
    		bhhd.ID_DonVi,
			bhhd.ID_Xe,
			bhhd.ID_PhieuTiepNhan,
    		bhhd.ChoThanhToan,
    		bhhd.ID_KhuyenMai,
    		bhhd.KhuyenMai_GhiChu,
    		bhhd.LoaiHoaDon,
			isnull(bhhd.PTThueHoaDon,0) as  PTThueHoaDon,
    		ISNULL(bhhd.KhuyeMai_GiamGia,0) AS KhuyeMai_GiamGia,
    		ISNULL(bhhd.DiemGiaoDich,0) AS DiemGiaoDich,
    		ISNULL(gb.ID,N'00000000-0000-0000-0000-000000000000') as ID_BangGia,
			ISNULL(vt.ID,N'00000000-0000-0000-0000-000000000000') as ID_ViTri,
    		ISNULL(vt.TenViTri,'') as TenPhongBan,
    		bhhd.MaHoaDon,
    		Case when hdt.MaHoaDon is null then '' else hdt.MaHoaDon end as MaHoaDonGoc,
    		bhhd.NgayLapHoaDon,
    		bhhd.NgayApDungGoiDV,
    		bhhd.HanSuDungGoiDV,
			dt.MaDoiTuong,
			ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
    		ISNULL(dt.TenDoiTuong_KhongDau, N'khach le') as TenDoiTuong_KhongDau,
			ISNULL(dt.TenDoiTuong_ChuCaiDau, N'kl') as TenDoiTuong_ChuCaiDau,
			ISNULL(dt.Email, N'') as Email,
			ISNULL(dt.DienThoai, N'') as DienThoai,
			ISNULL(dt.DiaChi, N'') as DiaChiKhachHang,
			ISNULL(tt.TenTinhThanh, N'') as KhuVuc,
			ISNULL(qh.TenQuanHuyen, N'') as PhuongXa,
			ISNULL(dv.TenDonVi, N'') as TenDonVi,
			ISNULL(dv.DiaChi, N'') as DiaChiChiNhanh,
			ISNULL(dv.SoDienThoai, N'') as DienThoaiChiNhanh,
			ISNULL(nv.TenNhanVien, N'') as TenNhanVien,
			ISNULL(nv.TenNhanVienKhongDau, N'') as TenNhanVienKhongDau,
    		ISNULL(dt.TongTichDiem,0) AS DiemSauGD,
    		ISNULL(gb.TenGiaBan,N'Bảng giá chung') AS TenBangGia,
    		bhhd.DienGiai,
    		bhhd.NguoiTao as NguoiTaoHD,
    		bhhd.TongChietKhau,
			bhhd.TongThanhToan,
			ISNULL(bhhd.TongThueKhachHang,0) as TongThueKhachHang,
			ISNULL(bhhd.TongTienThue,0) as TongTienThue,
			bhhd.TongTienHang,
			bhhd.TongGiamGia,
			bhhd.PhaiThanhToan,
			ISNULL(hdt.PhaiThanhToan,0) as TongTienHDTra,
    		a.ThuTuThe,
    		a.TienMat,
			a.TienATM,
			a.TienDoiDiem,
    		a.ChuyenKhoan,
    		a.KhachDaTra,
			ID_TaiKhoanPos,
			ID_TaiKhoanChuyenKhoan,
    		ISNULL(hdt.LoaiHoaDon,0) as LoaiHoaDonGoc,
    		Case When bhhd.ChoThanhToan = '1' then N'Phiếu tạm' when bhhd.ChoThanhToan = '0' then N'Hoàn thành' else N'Đã hủy' end as TrangThai
    		FROM
    		(
    			Select 
    			b.ID,
    			SUM(ISNULL(b.ThuTuThe, 0)) as ThuTuThe,
    			SUM(ISNULL(b.TienMat, 0)) as TienMat,
				SUM(ISNULL(b.TienATM, 0)) as TienATM,
    			SUM(ISNULL(b.TienCK, 0)) as ChuyenKhoan,
				SUM(ISNULL(b.TienDoiDiem, 0)) as TienDoiDiem,
    			SUM(ISNULL(b.TienThu, 0)) as KhachDaTra,
				max(b.ID_TaiKhoanPos) as ID_TaiKhoanPos,
				max(b.ID_TaiKhoanChuyenKhoan) as ID_TaiKhoanChuyenKhoan
    			from
    			(
    				Select 
    				bhhd.ID,
    				Case when qhd.TrangThai = 0 then 0 else Case when qhd.LoaiHoaDon = '11' then ISNULL(hdct.TienMat, 0) else ISNULL(hdct.TienMat, 0) * (-1) end end as TienMat,
					case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = '11' then case when TaiKhoanPOS = 1 then ISNULL(hdct.TienGui, 0) else 0 end else ISNULL(hdct.TienGui, 0) * (-1) end end as TienATM,
					case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = '11' then case when TaiKhoanPOS = 0 then ISNULL(hdct.TienGui, 0) else 0 end else ISNULL(hdct.TienGui, 0) * (-1) end end as TienCK,
    				Case when qhd.TrangThai = 0 then 0 else Case when qhd.LoaiHoaDon = '11' then ISNULL(hdct.ThuTuThe, 0) else ISNULL(hdct.ThuTuThe, 0) * (-1) end end as ThuTuThe,
					case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = 11 then 
							case when ISNULL(hdct.DiemThanhToan, 0) = 0 then 0 else ISNULL(hdct.Tienthu, 0) end
							else case when ISNULL(hdct.DiemThanhToan, 0) = 0 then 0 else -ISNULL(hdct.Tienthu, 0) end end end as TienDoiDiem,
    				Case when qhd.TrangThai = 0 then 0 else Case when qhd.LoaiHoaDon = '11' then ISNULL(hdct.Tienthu, 0) else ISNULL(hdct.Tienthu, 0) * (-1) end end as TienThu,
					case when qhd.TrangThai = 0 then '00000000-0000-0000-0000-000000000000' else case when TaiKhoanPOS = 1 then hdct.ID_TaiKhoanNganHang else '00000000-0000-0000-0000-000000000000' end end as ID_TaiKhoanPos,
					case when qhd.TrangThai = 0 then '00000000-0000-0000-0000-000000000000' else case when TaiKhoanPOS = 0 then hdct.ID_TaiKhoanNganHang else '00000000-0000-0000-0000-000000000000' end end as ID_TaiKhoanChuyenKhoan
    				from BH_HoaDon bhhd
    				left join Quy_HoaDon_ChiTiet hdct on bhhd.ID = hdct.ID_HoaDonLienQuan	
    				left join Quy_HoaDon qhd on hdct.ID_HoaDon = qhd.ID  
					left join DM_TaiKhoanNganHang tk on tk.ID= hdct.ID_TaiKhoanNganHang		
    				where bhhd.LoaiHoaDon = '19' and bhhd.NgayLapHoadon >= @timeStart and bhhd.NgayLapHoaDon < @timeEnd 
					and bhhd.ID_DonVi IN (Select * from splitstring(@ID_ChiNhanh))     			
    			) b
    			group by b.ID 
    		) as a
    		inner join BH_HoaDon bhhd on a.ID = bhhd.ID
    		left join BH_HoaDon hdt on bhhd.ID_HoaDon = hdt.ID
    		left join DM_DoiTuong dt on bhhd.ID_DoiTuong = dt.ID
    		left join DM_DonVi dv on bhhd.ID_DonVi = dv.ID
    		left join NS_NhanVien nv on bhhd.ID_NhanVien = nv.ID 
    		left join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
    		left join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
    		left join DM_GiaBan gb on bhhd.ID_BangGia = gb.ID
    		left join DM_ViTri vt on bhhd.ID_ViTri = vt.ID
			
    			left join 
    				(Select distinct hdXML.ID, 
    					 (
    						select qd.MaHangHoa +', '  AS [text()], hh.TenHangHoa +', '  AS [text()]
    						from BH_HoaDon_ChiTiet ct
    						join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    						join DM_HangHoa hh on  hh.ID= qd.ID_HangHoa
    						where ct.ID_HoaDon = hdXML.ID
    						For XML PATH ('')
    					) HoaDon_HangHoa
    				from BH_HoaDon hdXML) hdXMLOut on a.ID= hdXMLOut.ID
    		) as c
			left join Gara_DanhMucXe xe on c.ID_Xe= xe.ID
			where 
				((select count(Name) from @tblSearch b where     			
				c.MaHoaDon like '%'+b.Name+'%'
				or c.NguoiTaoHD like '%'+b.Name+'%'				
				or c.TenNhanVien like '%'+b.Name+'%'
				or c.TenNhanVienKhongDau like '%'+b.Name+'%'
				or c.DienGiai like '%'+b.Name+'%'
				or c.MaDoiTuong like '%'+b.Name+'%'		
				or c.TenDoiTuong like '%'+b.Name+'%'
				or c.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				or c.DienThoai like '%'+b.Name+'%'						
				or xe.BienSo like '%'+b.Name+'%'	
				or c.HoaDon_HangHoa like '%'+b.Name+'%'			
				)=@count or @count=0)	
    	ORDER BY c.NgayLapHoaDon DESC
END");

			Sql(@"ALTER PROCEDURE [dbo].[getlist_HoaDon_afterTraHang_DichVu]
   @IDChiNhanhs nvarchar(max) = null,
   @IDNhanViens nvarchar(max) = null,
   @DateFrom datetime = null,
   @DateTo datetime = null,
   @TextSearch nvarchar(max) = null,
   @CurrentPage int =null,
   @PageSize int = null
AS
BEGIN
	set nocount on;

	if isnull(@CurrentPage,'') =''
		set @CurrentPage = 0
	if isnull(@PageSize,'') =''
		set @PageSize = 10

	declare @sql nvarchar(max) ='', @sqlTemp nvarchar(max),  	
				@whereTemp nvarchar(max)= '', @whereTempOut nvarchar(max)= '', 
				@paramDefined nvarchar(max)=''

	
	set @whereTemp = N' where 1 = 1 and hd.ChoThanhToan = 0 and hd.LoaiHoaDon in (1,25) and
						(hdct.ID_ChiTietDinhLuong is null OR hdct.ID_ChiTietDinhLuong = hdct.ID)'
	set @whereTempOut= N' where 1 = 1  '

	set @paramDefined = N' @IDChiNhanhs_In nvarchar(max) ,
		   @IDNhanViens_In nvarchar(max) ,
		   @DateFrom_In datetime ,
		   @DateTo_In datetime ,
		   @TextSearch_In nvarchar(max) ,
		   @CurrentPage_In int =null,
		   @PageSize_In int  '

	if isnull(@IDChiNhanhs,'') !=''
	begin
		set @sqlTemp = N' DECLARE @tblChiNhanh table (ID nvarchar(max)) insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs_In) '
		set @whereTemp = CONCAT(@whereTemp,  N' and exists (select ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID) ')	
	end

	if isnull(@IDNhanViens,'') !=''
	begin
		set @sqlTemp = CONCAT(@sqlTemp, N'
			DECLARE @tblNhanVien table (ID uniqueidentifier)
			insert into @tblNhanVien select name from dbo.splitstring(@IDNhanViens_In)')
		set @whereTemp = CONCAT(@whereTemp,  ' and exists (select nv.ID from @tblNhanVien nv where hd.ID_NhanVien = nv.ID)')
	end

	if isnull(@TextSearch,'') !=''
	begin
		set @sqlTemp = CONCAT(@sqlTemp, N'
								DECLARE @tblSearch TABLE (Name [nvarchar](max))
								DECLARE @count int
								 INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch_In, '' '') where Name!='''' 
								 select @count =  (Select count(*) from @tblSearch)')
		set @whereTempOut = CONCAT(@whereTempOut,  ' and
					((select count(Name) from @tblSearch b where     			
					a.MaHoaDon like ''%''+b.Name+''%''								
					or dt.MaDoiTuong like ''%''+b.Name+''%''		
					or dt.TenDoiTuong like ''%''+b.Name+''%''
					or dt.TenDoiTuong_KhongDau like ''%''+b.Name+''%''
					or dt.DienThoai like ''%''+b.Name+''%''		
					or xe.BienSo like ''%''+b.Name+''%''			
					)=@count or @count=0)')
	end

	if isnull(@DateFrom,'') !=''
	begin		
		set @whereTemp= CONCAT(@whereTemp,  N' and hd.NgayLapHoaDon >= @DateFrom_In')
	end

	if isnull(@DateTo,'') !=''
	begin		
		set @DateTo = DATEADD(day, 1, @DateTo)
		set @whereTemp= CONCAT(@whereTemp,  N' and hd.NgayLapHoaDon < @DateTo_In')
	end

	set @sqlTemp = CONCAT(@sqlTemp, N'
			select *
			into #tblView
			from 
			(
				select a.*,
					row_number() over (order by a.NgayLapHoaDon desc) as Rn,
					COUNT(a.ID) OVER () as TotalRow,
					dt.DienThoai,
					xe.BienSo,
					ISNULL(dt.MaDoiTuong,''kl'' ) as MaDoiTuong,
					ISNULL(dt.TenDoiTuong,N''Khách lẻ'' ) as TenDoiTuong,
					ISNULL(dt.TenDoiTuong_KhongDau,''kl'' ) as TenDoiTuong_KhongDau,
					ISNULL(dt.TenDoiTuong_ChuCaiDau,''kl'' ) as TenDoiTuong_ChuCaiDau
				from
				(
				select 
					conlai.ID,
					conlai.ID_DoiTuong,
					max(conlai.ID_Xe) as ID_Xe,
					max(conlai.MaHoaDon) as MaHoaDon,
					max(conlai.NgayLapHoaDon) as NgayLapHoaDon,
					sum(conlai.SoLuongBan) as SoLuongBan,
					sum(conlai.SoLuongTra) as SoLuongTra,
					sum(conlai.SoLuongBan) - ISNULL(sum(conlai.SoLuongTra),0) as SoLuongConLai
				from
				(
					-- sum SLMua
    					Select 
    						hd.ID as ID,									
							hd.ID_DoiTuong,
							hd.ID_Xe,
							hd.MaHoaDon,
							hd.NgayLapHoaDon,							
    						sum(iif(hd.LoaiHoaDon = 1 
								or ((hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
								and (hdct.ID_ParentCombo is null or hdct.ID_ParentCombo= hdct.ID)) , 
							hdct.SoLuong,  isnull(xk.SoLuongXuat,0))) as SoLuongBan,						
    						0 as SoLuongTra
    					from BH_HoaDon hd   				
    					inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
						left join 
						(
							select SUM(ctxk.SoLuong) as SoLuongXuat, ctxk.ID_ChiTietGoiDV, hdxk.ID_HoaDon
							from BH_HoaDon_ChiTiet ctxk 
							join BH_HoaDon hdxk on ctxk.ID_HoaDon = hdxk.ID
							where hdxk.ID_HoaDon is not null
							and hdxk.LoaiHoaDon = 8 and hdxk.ChoThanhToan=0
							group by ctxk.ID_ChiTietGoiDV, hdxk.ID_HoaDon			
						) xk on hd.ID= xk.ID_HoaDon and hdct.ID= xk.ID_ChiTietGoiDV  						
						', @whereTemp, 
						' Group by hd.ID,hd.NgayLapHoaDon , hd.LoaiHoaDon,hd.ID_DoiTuong,hd.MaHoaDon, hd.ID_Xe
					
						union all
						-- sum SLTra
						select 
    						hd.ID_HoaDon as ID,			
							hd.ID_DoiTuong,
							''00000000-0000-0000-0000-000000000000'' as ID_Xe,
							'''' as MaHoaDon,
							null as NgayLapHoaDon,
    						0 as SoLuongBan,						
    						Sum(ISNULL(hdct.SoLuong, 0)) as SoLuongTra
    					from BH_HoaDon hd    				
    					inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon  
						where 1 = 1 and hd.ChoThanhToan = 0  and hd.loaihoadon = 6
						and (hdct.ID_ChiTietDinhLuong is null OR hdct.ID_ChiTietDinhLuong = hdct.ID)						
						and hdct.ChatLieu= 1
    					Group by hd.ID_HoaDon,hd.ID_DoiTuong
				) conlai group by conlai.ID, conlai.ID_DoiTuong						
			) a
			left join DM_DoiTuong dt on a.ID_DoiTuong = dt.ID 
			left join Gara_DanhMucXe xe on a.ID_Xe = xe.ID
			',
			@whereTempOut,  ' and a.SoLuongConLai > 0 ' ,
			') b where b.Rn between  (@CurrentPage_In * @PageSize_In) + 1 and @PageSize_In * (@CurrentPage_In + 1)')

	

		set @sql= CONCAT(@sql, N' select 
							tbl.ID,
							tbl.SoLuongTra,
    						tbl.SoLuongBan,
							tbl.TotalRow,
							tbl.MaDoiTuong,
							tbl.TenDoiTuong,
							tbl.DienThoai,
							tbl.BienSo,
    						hd.MaHoaDon,
    						hd.LoaiHoaDon,
    						hd.NgayLapHoaDon,   						
    						hd.ID_DoiTuong,	
    						hd.ID_HoaDon,
    						hd.ID_BangGia,
    						hd.ID_NhanVien,
    						hd.ID_DonVi,
							hd.ID_Xe,
    						hd.NguoiTao,	
    						hd.DienGiai,	 
							hd.PhaiThanhToan - isnull(thuchi.TienThu,0) as ConNo,
							isnull(thuchi.TienThu,0) as KhachDaTra,
							ISNULL(thuchi.ThuTuThe, 0) as ThuTuThe,
    						ISNULL(nv.TenNhanVien,'''' ) as TenNhanVien,							
    						ISNULL(hd.PhaiThanhToan, 0) as PhaiThanhToan,
    						ISNULL(hd.TongTienHang, 0) as TongTienHang,
    						ISNULL(hd.TongGiamGia, 0) as TongGiamGia,
							isnull(hd.TongChietKhau,0) as  TongChietKhau,
    						ISNULL(hd.DiemGiaoDich, 0) as DiemGiaoDich,							
							ISNULL(hd.TongTienThue, 0) as TongTienThue,						
							isnull(hd.PTThueHoaDon,0) as  PTThueHoaDon,
							ISNULL(hd.TongThueKhachHang, 0) as TongThueKhachHang,	
							isnull(hd.TongTienThueBaoHiem,0) as  TongTienThueBaoHiem,
							isnull(hd.TongTienBHDuyet,0) as  TongTienBHDuyet,
							isnull(hd.SoVuBaoHiem,0) as  SoVuBaoHiem,
							isnull(hd.PTThueBaoHiem,0) as  PTThueBaoHiem,
							isnull(hd.KhauTruTheoVu,0) as  KhauTruTheoVu,
							isnull(hd.GiamTruBoiThuong,0) as  GiamTruBoiThuong,
							isnull(hd.PTGiamTruBoiThuong,0) as  PTGiamTruBoiThuong,
							isnull(hd.BHThanhToanTruocThue,0) as  BHThanhToanTruocThue,
							isnull(hd.PhaiThanhToanBaoHiem,0) as  PhaiThanhToanBaoHiem
						from #tblView tbl
						join BH_HoaDon hd on tbl.ID= hd.ID
						left join NS_NhanVien nv on hd.ID_NhanVien= nv.ID
						left join (
							select 
								tblQuy.ID_HoaDonLienQuan,
								sum(tblQuy.ThuTuThe) as ThuTuThe,
								sum(tblQuy.TienThu) as TienThu
							from
							(
								---- Thu tu HDMua
								select qct.ID_HoaDonLienQuan,
									case when qhd.TrangThai= 0 then 0 else SUM(iif(qct.HinhThucThanhToan= 4, qct.TienThu,0)) end as ThuTuThe,							
									Case when qhd.TrangThai = 0 then 0 else 
										Case when qhd.LoaiHoaDon = 11 then SUM(ISNULL(qct.Tienthu, 0)) else - SUM(ISNULL(qct.Tienthu, 0)) end end as TienThu
								from Quy_HoaDon_ChiTiet qct					
								join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID		
								join #tblView tbl on qct.ID_HoaDonLienQuan = tbl.ID				
								group by qct.ID_HoaDonLienQuan, qhd.TrangThai,qhd.LoaiHoaDon
							) tblQuy group by tblQuy.ID_HoaDonLienQuan
						) thuchi on tbl.ID= thuchi.ID_HoaDonLienQuan')

		--print @sqlTemp
		--print @sql

		set @sql = CONCAT(@sqlTemp, @sql)
				
		exec sp_executesql @sql, @paramDefined, 
							@IDChiNhanhs_In = @IDChiNhanhs,
							@IDNhanViens_In = @IDNhanViens,
							@DateFrom_In = @DateFrom,
							@DateTo_In = @DateTo,
							@TextSearch_In = @TextSearch,
							@CurrentPage_In = @CurrentPage,
							@PageSize_In = @PageSize
    	
END");

			Sql(@"ALTER PROCEDURE [dbo].[getlist_HoaDonBanHang]	
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @maHD [nvarchar](max),
	@ID_NhanVienLogin uniqueidentifier,
	@NguoiTao nvarchar(max),
	@IDViTris nvarchar(max),
	@IDBangGias nvarchar(max),
	@TrangThai nvarchar(max),
	@PhuongThucThanhToan nvarchar(max),
	@ColumnSort varchar(max),
	@SortBy varchar(max),
	@CurrentPage int,
	@PageSize int,
	@LaHoaDonSuaChua nvarchar(10),
	@BaoHiem int
AS
BEGIN

  set nocount on;
 declare @tblNhanVien table (ID uniqueidentifier)
	insert into @tblNhanVien
	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanh,'HoaDon_XemDS_PhongBan','HoaDon_XemDS_HeThong');

	declare @tblChiNhanh table (ID varchar(40))
	insert into @tblChiNhanh
	select Name from dbo.splitstring(@ID_ChiNhanh);

	declare @tblPhuongThuc table (PhuongThuc varchar(4))
	insert into @tblPhuongThuc
	select Name from dbo.splitstring(@PhuongThucThanhToan)

	declare @tblTrangThai table (TrangThaiHD varchar(40))
	insert into @tblTrangThai
	select Name from dbo.splitstring(@TrangThai);


	declare @tblViTri table (ID varchar(40))
	insert into @tblViTri
	select Name from dbo.splitstring(@IDViTris)

	declare @tblBangGia table (ID varchar(40))
	insert into @tblBangGia
	select Name from dbo.splitstring(@IDBangGias)

	declare @tblLoaiHoaDon table (Loai varchar(40))
	insert into @tblLoaiHoaDon
	select Name from dbo.splitstring(@LaHoaDonSuaChua)

	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@maHD, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);

	CREATE TABLE #TempIndex (	
		ID uniqueidentifier PRIMARY KEY, 
    	ID_DoiTuong uniqueidentifier,
    	ID_BaoHiem uniqueidentifier, 
    	ID_HoaDon uniqueidentifier, 
    	NgayLapHoaDon datetime,
    	ChoThanhToan bit
	)
		
	insert into #TempIndex WITH(TABLOCKX)
	select ID, ID_DoiTuong, ID_BaoHiem, ID_HoaDon,NgayLapHoaDon, ChoThanhToan
	from BH_HoaDon hd
	where hd.ID_DonVi in (select ID from @tblChiNhanh)
	and hd.NgayLapHoaDon between @timeStart and  @timeEnd 
	and hd.LoaiHoaDon in (1,25);
	

	with data_cte
	as(
	select c.*, iif(c.ChoThanhToan is null, 0,iif( c.ConNo1 - c.TongTienHDTra > 0, c.ConNo1 - c.TongTienHDTra,0)) as ConNo
	from
	(
	select 
			hd.ID,
    		hd.ID_DoiTuong,
    			
    			CASE 
    				WHEN dt.TheoDoi IS NULL THEN 
    					CASE WHEN dt.ID IS NULL THEN '0' ELSE '1' END
    				ELSE dt.TheoDoi
    			END AS TheoDoi,
    		hd.ID_HoaDon,
    		hd.ID_NhanVien,
    		hd.ID_DonVi,
			hd.ID_BaoHiem,
			hd.ID_PhieuTiepNhan,
    		hd.ChoThanhToan,
    		hd.ID_KhuyenMai,
    		hd.KhuyenMai_GhiChu,
    		hd.LoaiHoaDon,

			isnull(hd.TongThueKhachHang,0) as  TongThueKhachHang,
			isnull(hd.CongThucBaoHiem,0) as  CongThucBaoHiem,
			isnull(hd.GiamTruThanhToanBaoHiem,0) as  GiamTruThanhToanBaoHiem,
			isnull(hd.PTThueHoaDon,0) as  PTThueHoaDon,
			isnull(hd.TongTienThueBaoHiem,0) as  TongTienThueBaoHiem,
			isnull(hd.TongTienBHDuyet,0) as  TongTienBHDuyet,
			isnull(hd.SoVuBaoHiem,0) as  SoVuBaoHiem,
			isnull(hd.PTThueBaoHiem,0) as  PTThueBaoHiem,
			isnull(hd.KhauTruTheoVu,0) as  KhauTruTheoVu,
			isnull(hd.GiamTruBoiThuong,0) as  GiamTruBoiThuong,
			isnull(hd.PTGiamTruBoiThuong,0) as  PTGiamTruBoiThuong,
			isnull(hd.BHThanhToanTruocThue,0) as  BHThanhToanTruocThue,

    		ISNULL(hd.KhuyeMai_GiamGia,0) AS KhuyeMai_GiamGia,
    		ISNULL(hd.DiemGiaoDich,0) AS DiemGiaoDich,
			ISNULL(hd.ID_BangGia,N'00000000-0000-0000-0000-000000000000') as ID_BangGia,
			ISNULL(hd.ID_ViTri,N'00000000-0000-0000-0000-000000000000') as ID_ViTri,
    		ISNULL(vt.TenViTri,'') as TenPhongBan,
    		hd.MaHoaDon,
    		Case when hdt.MaHoaDon is null then '' else hdt.MaHoaDon end as MaHoaDonGoc,
    		hd.NgayLapHoaDon,
			dt.MaDoiTuong,
			ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
    		ISNULL(dt.TenDoiTuong_KhongDau, N'khach le') as TenDoiTuong_KhongDau,
			ISNULL(dt.TenDoiTuong_ChuCaiDau, N'kl') as TenDoiTuong_ChuCaiDau,
			dt.NgaySinh_NgayTLap,
			dt.MaSoThue,
			dt.TaiKhoanNganHang,
			ISNULL(dt.Email, N'') as Email,
			ISNULL(dt.DienThoai, N'') as DienThoai,
			ISNULL(dt.DiaChi, N'') as DiaChiKhachHang,
			isnull(bh.MaDoiTuong,'') as MaBaoHiem,
			isnull(bh.TenDoiTuong,'') as TenBaoHiem,
			isnull(bh.DienThoai,'') as BH_SDT,
			isnull(bh.DiaChi,'') as BH_DiaChi,
			isnull(bh.Email,'') as BH_Email,
			isnull(bh.TenDoiTuong_KhongDau,'') as TenBaoHiem_KhongDau,

			iif(hd.ID_BaoHiem is null,'', tn.NguoiLienHeBH) as LienHeBaoHiem,
			iif(hd.ID_BaoHiem is null,'', tn.SoDienThoaiLienHeBH) as SoDienThoaiLienHeBaoHiem,
			
			dt.ID_TinhThanh, 
			dt.ID_QuanHuyen,
			ISNULL(tt.TenTinhThanh, N'') as KhuVuc,
			ISNULL(qh.TenQuanHuyen, N'') as PhuongXa,
			ISNULL(dv.TenDonVi, N'') as TenDonVi,
			ISNULL(dv.DiaChi, N'') as DiaChiChiNhanh,
			ISNULL(dv.SoDienThoai, N'') as DienThoaiChiNhanh,
			ISNULL(nv.TenNhanVien, N'') as TenNhanVien,
			ISNULL(nv.TenNhanVienKhongDau, N'') as TenNhanVienKhongDau,
    		ISNULL(dt.TongTichDiem,0) AS DiemSauGD,
    		ISNULL(gb.TenGiaBan,N'Bảng giá chung') AS TenBangGia,
    		hd.DienGiai,
			dbo.FUNC_ConvertStringToUnsign(hd.DienGiai) as DienGiaiKhongDau,
    		hd.NguoiTao as NguoiTaoHD,
			ISNULL(hd.TongChietKhau,0) as TongChietKhau,
			ISNULL(hd.TongTienHang,0) as TongTienHang,
			ISNULL(hd.ChiPhi,0) as TongChiPhi, --- chiphi cuahang phaitra
			ISNULL(hd.TongGiamGia,0) as TongGiamGia,
			ISNULL(hd.TongTienThue,0) as TongTienThue,
			ISNULL(hd.PhaiThanhToan,0) as PhaiThanhToan,
			ISNULL(hd.TongThanhToan,0) as TongThanhToan,
			ISNULL(hd.PhaiThanhToanBaoHiem,0) as PhaiThanhToanBaoHiem,
			iif(hdt.LoaiHoaDon=6,ISNULL(hdt.TongThanhToan,0),0) as TongTienHDTra, -- hdgoc: co the la baogia/hoactrahang
			ISNULL(hd.TongThanhToan,0) - ISNULL(a.DaThanhToan,0) as ConNo1,		
			ISNULL(a.ThuTuThe,0) as ThuTuThe,
			ISNULL(a.TienMat,0) as TienMat,
			ISNULL(a.TienATM,0) as TienATM,
			ISNULL(a.ChuyenKhoan,0) as ChuyenKhoan,
			ISNULL(a.TienDoiDiem,0) as TienDoiDiem,
			ISNULL(a.TienDatCoc,0) as TienDatCoc,
			ISNULL(a.GiaTriSDDV,0) as GiaTriSDDV,
			ISNULL(a.GiamGiaCT,0) as GiamGiaCT,
			ISNULL(a.ThanhTienChuaCK,0) as ThanhTienChuaCK,
			ISNULL(a.KhachDaTra,0) as KhachDaTra,
			ISNULL(a.DaThanhToan,0) as DaThanhToan,
			ISNULL(a.BaoHiemDaTra,0) as BaoHiemDaTra,

			cx.MaDoiTuong as MaChuXe,
			cx.TenDoiTuong as ChuXe,

			hd.PhaiThanhToan - ISNULL(a.KhachDaTra,0) as KhachConNo,
			isnull(hd.PhaiThanhToanBaoHiem,0) - ISNULL(a.BaoHiemDaTra,0) as BHConNo,

			hd.ID_Xe,
			isnull(tn.MaPhieuTiepNhan,'') as MaPhieuTiepNhan,
			isnull(xe.BienSo,'') as BienSo,
    		ISNULL(hdt.LoaiHoaDon,0) as LoaiHoaDonGoc,
    		Case When hd.ChoThanhToan = '1' then N'Phiếu tạm' when hd.ChoThanhToan = '0' then N'Hoàn thành' else N'Đã hủy' end as TrangThai,
			case  hd.ChoThanhToan
				when 1 then '1'
				when 0 then '0'
			else '4' end as TrangThaiHD,
			iif(hd.ID_PhieuTiepNhan is null, '0','1') as LaHoaDonSuaChua,
			case when a.TienMat > 0 then
				case when a.TienATM > 0 then	
					case when a.ChuyenKhoan > 0 then
						case when a.ThuTuThe > 0 then '1,2,3,4' else '1,2,3' end												
						else 
							case when a.ThuTuThe > 0 then  '1,2,4' else '1,2' end end
						else
							case when a.ChuyenKhoan > 0 then 
								case when a.ThuTuThe > 0 then '1,3,4' else '1,3' end
								else 
										case when a.ThuTuThe > 0 then '1,4' else '1' end end end
				else
					case when a.TienATM > 0 then
						case when a.ChuyenKhoan > 0 then
								case when a.ThuTuThe > 0 then '2,3,4' else '2,3' end	
								else 
									case when a.ThuTuThe > 0 then '2,4' else '2' end end
							else 		
								case when a.ChuyenKhoan > 0 then
									case when a.ThuTuThe > 0 then '3,4' else '3' end
									else 
									case when a.ThuTuThe > 0 then '4' else '5' end end end end
									
						as PTThanhToan,
			ID_TaiKhoanPos,
			ID_TaiKhoanChuyenKhoan
	from BH_HoaDon hd
	left join
	(
	
	Select 
    			b.ID,    			
    			SUM(ISNULL(b.TienMat, 0)) as TienMat,
				SUM(ISNULL(b.TienATM, 0)) as TienATM,
    			SUM(ISNULL(b.TienCK, 0)) as ChuyenKhoan,
				SUM(ISNULL(b.TienDoiDiem, 0)) as TienDoiDiem,
				SUM(ISNULL(b.ThuTuThe, 0)) as ThuTuThe,
    			SUM(ISNULL(b.TienThu, 0)) as DaThanhToan, --- = khach + baohiem tra
				SUM(ISNULL(b.KhachDaTra, 0)) as KhachDaTra,
				SUM(ISNULL(b.BaoHiemDaTra, 0)) as BaoHiemDaTra,
				SUM(ISNULL(b.GiaTriSDDV, 0)) as GiaTriSDDV,
				SUM(ISNULL(b.GiamGiaCT, 0)) as GiamGiaCT,
				SUM(ISNULL(b.ThanhTien, 0)) as ThanhTienChuaCK,
				max(b.ID_TaiKhoanPOS) as ID_TaiKhoanPos,
				max(b.ID_TaiKhoanChuyenKhoan) as ID_TaiKhoanChuyenKhoan,
				SUM(ISNULL(b.TienDatCoc, 0)) as TienDatCoc
    			from
    			(
					---- quyct of thishoadon
					select 
						soquyHD.ID_HoaDonLienQuan as ID,
						sum(soquyHD.TienMat) as TienMat,
						sum(soquyHD.TienATM) as TienATM,
						sum(soquyHD.TienCK) as TienCK,
						sum(soquyHD.TienDoiDiem) as TienDoiDiem,
						sum(soquyHD.ThuTuThe) as ThuTuThe,					
						sum(soquyHD.TienThu) as TienThu,
						0 as GiaTriSDDV,
						0 as GiamGiaCT,
						0 as ThanhTien,					
						max(soquyHD.ID_TaiKhoanPos) as ID_TaiKhoanPos,
						max(soquyHD.ID_TaiKhoanChuyenKhoan) as ID_TaiKhoanChuyenKhoan,
						sum(soquyHD.KhachDaTra) as KhachDaTra,
						sum(soquyHD.BaoHiemDaTra) as BaoHiemDaTra,
						sum(soquyHD.TienDatCoc) as TienDatCoc					
				from
				(
						select 
							soquy.ID_HoaDonLienQuan,
							soquy.ID_DoiTuong,					
							sum(isnull(soquy.TienMat,0)) as TienMat,
							sum(isnull(soquy.TienATM,0)) as TienATM,
							sum(isnull(soquy.TienCK,0)) as TienCK,
							sum(isnull(soquy.TienDoiDiem,0)) as TienDoiDiem,
							sum(isnull(soquy.ThuTuThe,0)) as ThuTuThe,
							sum(isnull(soquy.TienDatCoc,0)) as TienDatCoc,
							sum(isnull(soquy.TienThu,0)) as TienThu,
							max(soquy.ID_TaiKhoanPos) as ID_TaiKhoanPos,
							max(soquy.ID_TaiKhoanChuyenKhoan) as ID_TaiKhoanChuyenKhoan,
							iif(soquy.LoaiDoiTuong =1, sum(isnull(soquy.TienThu,0)),0) as KhachDaTra,
							iif(soquy.LoaiDoiTuong =3, sum(isnull(soquy.TienThu,0)),0) as BaoHiemDaTra
						from
						(
								select
									qct.ID_HoaDonLienQuan,
									qct.ID_DoiTuong,
									qct.ID_HoaDon,
									dt.LoaiDoiTuong,
									case  when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=1, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=1, -qct.TienThu,0) end as TienMat,
									case  when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=2, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=2, -qct.TienThu,0) end as TienATM,
									case  when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=3, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=3, -qct.TienThu,0) end as TienCK,
									case  when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=5, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=5, -qct.TienThu,0) end as TienDoiDiem,
									case  when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=4, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=4, -qct.TienThu,0) end as ThuTuThe,
									case  when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=6, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=6, -qct.TienThu,0) end as TienDatCoc,
									iif(qhd.LoaiHoaDon = 11, qct.TienThu, - qct.TienThu) as TienThu,
									iif(TaiKhoanPOS = 1 , qct.ID_TaiKhoanNganHang,  '00000000-0000-0000-0000-000000000000' ) as ID_TaiKhoanPos,
									iif(TaiKhoanPOS = 0 , qct.ID_TaiKhoanNganHang,  '00000000-0000-0000-0000-000000000000' ) as ID_TaiKhoanChuyenKhoan						
								from Quy_HoaDon_ChiTiet qct
								join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
								join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
								left join DM_DoiTuong dt on qct.ID_DoiTuong = dt.ID
								left join DM_TaiKhoanNganHang tk on tk.ID= qct.ID_TaiKhoanNganHang 
								where (qhd.TrangThai= 1 or qhd.TrangThai is null) and dt.LoaiDoiTuong in (1,3)	
								and qhd.ID_DonVi in (select ID from @tblChiNhanh)
								and hd.NgayLapHoaDon between @timeStart and @timeEnd
								and hd.LoaiHoaDon in (1,25)
						) soquy				
						group by soquy.ID_HoaDonLienQuan, soquy.ID_DoiTuong,soquy.LoaiDoiTuong
				) soquyHD 					
				group by soquyHD.ID_HoaDonLienQuan

			Union all
						---- get TongThu from HDDatHang: chi get hdXuly first
    					select 
							ID,
							TienMat, TienATM,ChuyenKhoan,
							TienDoiDiem, ThuTuThe, TienThu,
							0 AS GiaTriSDDV,
							0 as GiamGiaCT,
							0 as ThanhTien,
							'00000000-0000-0000-0000-000000000000' as ID_TaiKhoanPos,
							'00000000-0000-0000-0000-000000000000' as ID_TaiKhoanChuyenKhoan,
							TienThu as KhachDaTra,
							0 as BaoHiemDaTra,
							TienDatCoc

						from
						(	
								Select 
										ROW_NUMBER() OVER(PARTITION BY ID_HoaDonLienQuan ORDER BY NgayLapHoaDon ASC) AS isFirst,						
    									d.ID,
										ID_HoaDonLienQuan,
										d.NgayLapHoaDon,
    									sum(d.TienMat) as TienMat,
    									SUM(ISNULL(d.TienATM, 0)) as TienATM,
    									SUM(ISNULL(d.TienCK, 0)) as ChuyenKhoan,
										SUM(ISNULL(d.TienDoiDiem, 0)) as TienDoiDiem,
										sum(d.ThuTuThe) as ThuTuThe,
    									sum(d.TienThu) as TienThu,
										sum(d.TienDatCoc) as TienDatCoc
									
    								FROM
    								(
									
											select hd.ID, hd.NgayLapHoaDon,
												qct.ID_HoaDonLienQuan,
												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0), iif(qct.HinhThucThanhToan=1, -qct.TienThu, 0)) as TienMat,
												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0), iif(qct.HinhThucThanhToan=2, -qct.TienThu, 0)) as TienATM,
												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0), iif(qct.HinhThucThanhToan=3, -qct.TienThu, 0)) as TienCK,
												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0), iif(qct.HinhThucThanhToan=5, -qct.TienThu, 0)) as TienDoiDiem,
												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0), iif(qct.HinhThucThanhToan=4, -qct.TienThu, 0)) as ThuTuThe,
												iif(qhd.LoaiHoaDon = 11, qct.TienThu, -qct.TienThu) as TienThu,
												iif(qct.HinhThucThanhToan=6,qct.TienThu,0) as TienDatCoc
											from Quy_HoaDon_ChiTiet qct
											join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
											join BH_HoaDon hdd on hdd.ID= qct.ID_HoaDonLienQuan
											join #TempIndex hd on hd.ID_HoaDon= hdd.ID
											where hdd.LoaiHoaDon = '3' 	
											and hd.ChoThanhToan = 0
											and (qhd.TrangThai= 1 Or qhd.TrangThai is null)
    								) d group by d.ID,d.NgayLapHoaDon,ID_HoaDonLienQuan						
						) thuDH
						where isFirst= 1

					union all

					-- tong giatri sudung goiudv
					select 
						ctsd.ID_HoaDon as ID,
						0 as TienMat,
						0 as TienATM,
						0 as ChuyenKhoan,
						0 as TienDoiDiem,
						0 as ThuTuThe,						
						0 as TienThu,
						--- tinh %giamgia cua hoadon --> thanhtien moi dichvu sau giamgia
						ctsd.SoLuong * (ct.DonGia - ct.TienChietKhau) * ( 1 -  gdv.TongGiamGia/iif(gdv.TongTienHang =0,1,gdv.TongTienHang))  as GiaTriSDDV,
						0 as GiamGiaCT,
						0 as ThanhTien,
						'00000000-0000-0000-0000-000000000000' as ID_TaiKhoanPos,
						'00000000-0000-0000-0000-000000000000' as ID_TaiKhoanChuyenKhoan,
						0 as KhachDaTra,
						0 as BaoHiemDaTra,
						0 as TienDatCoc
					from BH_HoaDon_ChiTiet ctsd
					join BH_HoaDon_ChiTiet ct on ctsd.ID_ChiTietGoiDV= ct.ID
					join BH_HoaDon gdv on ct.ID_HoaDon= gdv.ID
					where  gdv.LoaiHoaDon = 19									
					and 
					 (ctsd.ID_ChiTietDinhLuong= ctsd.ID or ctsd.ID_ChiTietDinhLuong is null)-- khong lay TPDinhLuong
					and exists 
					(				
						select ID from #TempIndex hd where ctsd.ID_HoaDon=  hd.ID
					)

					union all
					---- sum cthd
					select 
						ct.ID_HoaDon,
						0 as TienMat,
						0 as TienATM,
						0 as ChuyenKhoan,
						0 as TienDoiDiem,
						0 as ThuTuThe,
						0 as TienThu,
						0 as GiaTriSDDV,
						ct.SoLuong * ct.TienChietKhau as GiamGiaCT,
						ct.SoLuong * ct.DonGia  as ThanhTien,
						'00000000-0000-0000-0000-000000000000' as ID_TaiKhoanPos,
						'00000000-0000-0000-0000-000000000000' as ID_TaiKhoanChuyenKhoan,
						0 as KhachDaTra,
						0 as BaoHiemDaTra,
						0 as TienDatCoc
					from BH_HoaDon_ChiTiet ct
					where (ct.ID_ChiTietDinhLuong= ct.ID or ct.ID_ChiTietDinhLuong is null)
					and (ct.ID_ParentCombo= ct.ID or ct.ID_ParentCombo is null)
					and exists 
					(
						select ID from #TempIndex hd where ct.ID_HoaDon=  hd.ID
					)				
					
	) b group by b.ID
	) a on hd.ID= a.ID
	left join BH_HoaDon hdt on hd.ID_HoaDon = hdt.ID
    	left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
		left join DM_DoiTuong bh on hd.ID_BaoHiem = bh.ID and bh.LoaiDoiTuong = 3
    	left join DM_DonVi dv on hd.ID_DonVi = dv.ID
    	left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID 
    	left join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
    	left join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
    	left join DM_GiaBan gb on hd.ID_BangGia = gb.ID
    	left join DM_ViTri vt on hd.ID_ViTri = vt.ID    	
		left join Gara_PhieuTiepNhan tn on hd.ID_PhieuTiepNhan = tn.ID
		left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID	
		left join DM_DoiTuong cx on xe.ID_KhachHang= cx.ID	
		where (@IDViTris ='' or exists (select ID from @tblViTri vt2 where vt2.ID= vt.ID))
		AND ((@BaoHiem = 0 AND 1 = 0) OR (@BaoHiem = 1 AND hd.ID_BaoHiem IS NOT NULL) OR (@BaoHiem = 2 AND hd.ID_BaoHiem IS NULL)
		OR @BaoHiem = 3 AND 1 = 1)
		and (@IDBangGias='' or exists (select bg.ID from @tblBangGia bg where bg.ID= gb.ID))	
		and
		(exists( select * from @tblNhanVien nv2 where nv2.ID= nv.ID or hd.NguoiTao = @NguoiTao))
		and hd.LoaiHoaDon in (select Loai from @tblLoaiHoaDon)
		and exists 
				(
				select ID from #TempIndex hd2 where hd.ID=  hd2.ID
				)
				and
				((select count(Name) from @tblSearch b where     			
				hd.MaHoaDon like '%'+b.Name+'%'
				or hd.NguoiTao like '%'+b.Name+'%'				
				or nv.TenNhanVien like '%'+b.Name+'%'
				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
				or hd.DienGiai like '%'+b.Name+'%'
				or dt.MaDoiTuong like '%'+b.Name+'%'		
				or dt.TenDoiTuong like '%'+b.Name+'%'
				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				or dt.DienThoai like '%'+b.Name+'%'		
				or tn.MaPhieuTiepNhan like '%'+b.Name+'%'
				or xe.BienSo like '%'+b.Name+'%'	
				or bh.MaDoiTuong like '%'+b.Name+'%'
				or bh.TenDoiTuong like '%'+b.Name+'%'	
				or bh.TenDoiTuong_KhongDau like '%'+b.Name+'%'	
				or cx.MaDoiTuong like '%'+b.Name+'%'
				or cx.TenDoiTuong like '%'+b.Name+'%'	
				or cx.TenDoiTuong_KhongDau like '%'+b.Name+'%'	
				)=@count or @count=0)	
) c
 where 
		c.TrangThaiHD in  (select TrangThaiHD from @tblTrangThai )
		and ( exists(SELECT Name FROM splitstring(c.PTThanhToan) pt join @tblPhuongThuc pt2 on pt.Name = pt2.PhuongThuc))
					
),
		count_cte
		as (
			select count(ID) as TotalRow,
				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
				sum(TongTienHang) as SumTongTienHang,
				sum(TongGiamGia) as SumTongGiamGia,
				sum(KhachDaTra) as SumKhachDaTra,
				sum(DaThanhToan) as SumDaThanhToan,
				sum(BaoHiemDaTra) as SumBaoHiemDaTra,
				sum(KhuyeMai_GiamGia) as SumKhuyeMai_GiamGia,
				sum(TongChiPhi) as SumTongChiPhi,
				sum(TongTienHDTra) as SumTongTongTienHDTra,
				sum(PhaiThanhToan) as SumPhaiThanhToan,
				sum(PhaiThanhToanBaoHiem) as SumPhaiThanhToanBaoHiem,
				sum(TongThanhToan) as SumTongThanhToan,
				sum(TienDoiDiem) as SumTienDoiDiem,
				sum(ThuTuThe) as SumThuTuThe,
				sum(TienDatCoc) as SumTienCoc,
				sum(ThanhTienChuaCK) as SumThanhTienChuaCK,
				sum(GiamGiaCT) as SumGiamGiaCT,
				sum(TienMat) as SumTienMat,
				sum(TienATM) as SumPOS,
				sum(ChuyenKhoan) as SumChuyenKhoan,
				sum(GiaTriSDDV) as TongGiaTriSDDV,
				sum(TongTienThue) as SumTongTienThue,
				sum(ConNo) as SumConNo,

				sum(TongTienThueBaoHiem) as SumTongTienThueBaoHiem,
				sum(TongTienBHDuyet) as SumTongTienBHDuyet,
				sum(KhauTruTheoVu) as SumKhauTruTheoVu,
				sum(GiamTruBoiThuong) as SumGiamTruBoiThuong,
				sum(BHThanhToanTruocThue) as SumBHThanhToanTruocThue
				
			from data_cte
		)
		select dt.*, cte.*
		from data_cte dt
		cross join count_cte cte
		order by 
			case when @SortBy <> 'ASC' then 0
			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end ASC,
			case when @SortBy <> 'DESC' then 0
			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end DESC,
			case when @SortBy <> 'ASC' then 0
			when @ColumnSort='ConNo' then ConNo end ASC,
			case when @SortBy <> 'DESC' then 0
			when @ColumnSort='ConNo' then ConNo end DESC,
			case when @SortBy <>'ASC' then ''
			when @ColumnSort='MaHoaDon' then MaHoaDon end ASC,
			case when @SortBy <>'DESC' then ''
			when @ColumnSort='MaHoaDon' then MaHoaDon end DESC,
			case when @SortBy <>'ASC' then ''
			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end ASC,
			case when @SortBy <>'DESC' then ''
			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end DESC,
			case when @SortBy <> 'ASC' then 0
			when @ColumnSort='TongTienHang' then TongTienHang end ASC,
			case when @SortBy <> 'DESC' then 0
			when @ColumnSort='TongTienHang' then TongTienHang end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='GiamGia' then TongGiamGia end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='GiamGia' then TongGiamGia end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='KhachDaTra' then KhachDaTra end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='KhachDaTra' then KhachDaTra end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TienMat' then TienMat end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TienMat' then TienMat end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='ChuyenKhoan' then ChuyenKhoan end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='ChuyenKhoan' then ChuyenKhoan end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TienATM' then TienATM end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TienATM' then TienATM end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='GiaTriSDDV' then GiaTriSDDV end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='GiaTriSDDV' then GiaTriSDDV end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='ThuTuThe' then ThuTuThe end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='ThuTuThe' then ThuTuThe end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TienDatCoc' then TienDatCoc end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TienDatCoc' then TienDatCoc end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='BaoHiemDaTra' then BaoHiemDaTra end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='BaoHiemDaTra' then BaoHiemDaTra end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='PhaiThanhToanBaoHiem' then PhaiThanhToanBaoHiem end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='PhaiThanhToanBaoHiem' then PhaiThanhToanBaoHiem end DESC ,

			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TongTienThueBaoHiem' then TongTienThueBaoHiem end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TongTienThueBaoHiem' then TongTienThueBaoHiem end DESC,			
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='KhauTruTheoVu' then KhauTruTheoVu end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='KhauTruTheoVu' then KhauTruTheoVu end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='GiamTruBoiThuong' then GiamTruBoiThuong end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='GiamTruBoiThuong' then GiamTruBoiThuong end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='BHThanhToanTruocThue' then BHThanhToanTruocThue end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='BHThanhToanTruocThue' then BHThanhToanTruocThue end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TongTienBHDuyet' then TongTienBHDuyet end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TongTienBHDuyet' then TongTienBHDuyet end DESC					
			
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY; 
		

		--drop table #TempIndex
		END");

			Sql(@"ALTER PROCEDURE [dbo].[GetList_HoaDonNhapHang]
    @TextSearch [nvarchar](max),
    @LoaiHoaDon varchar(50), ---- dùng chung cho nhập hàng + trả hàng nhập + nhập kho nội bộ
    @IDChiNhanhs [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @TrangThais [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int],
    @ColumnSort [nvarchar](max),
    @SortBy [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	declare @tblChiNhanh table (ID varchar(40))
    	insert into @tblChiNhanh
    	select Name from dbo.splitstring(@IDChiNhanhs)

		declare @tblLoaiHD table (Loai int)
    	insert into @tblLoaiHD
    	select Name from dbo.splitstring(@LoaiHoaDon)
    
    
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);
    
    	with data_cte
    	as (
    	select hdQuy.*	, hdQuy.PhaiThanhToan - hdQuy.KhachDaTra as ConNo
    	from
    	(	
    	select hd.id, hd.ID_HoaDon, hd.MaHoaDon, hd.LoaiHoaDon, hd.DienGiai, hd.PhaiThanhToan, hd.ChoThanhToan,
    	hd.NgayLapHoaDon, hd.ID_NhanVien, hd.ID_BangGia, hd.TongTienHang, hd.TongChietKhau, hd.TongGiamGia, hd.TongChiPhi,
    	hd.TongTienThue, hd.TongThanhToan, hd.ID_DoiTuong, 
		ctHD.ThanhTienChuaCK,
		ctHD.GiamGiaCT,	
		iif(@LoaiHoaDon='7', -isnull(quy.TienThu,0),  isnull(quy.TienThu,0))  as KhachDaTra,
		isnull(quy.TienMat,0) as TienMat,
		isnull(quy.ChuyenKhoan,0) as ChuyenKhoan,
		isnull(quy.TienATM,0) as TienATM,
		isnull(quy.TienDatCoc,0) as TienDatCoc,
		hd.NguoiTao, hd.NguoiTao as NguoiTaoHD,
    	dv.TenDonVi,hd.ID_DonVi,
		isnull(hd.PTThueHoaDon,0) as PTThueHoaDon,
    	isnull(dv.SoDienThoai,'') as DienThoaiChiNhanh,
    	isnull(dv.DiaChi,'') as DiaChiChiNhanh,
		iif(hd.LoaiHoaDon = 13, iif(hd.ID_DoiTuong='00000000-0000-0000-0000-000000000002',4,dt.LoaiDoiTuong), dt.LoaiDoiTuong) as LoaiDoiTuong,
		---- nhapnoibo: lay nhacc/nhanvien chung 1 cot
		iif(hd.LoaiHoaDon = 13, iif(hd.ID_DoiTuong='00000000-0000-0000-0000-000000000002',nv.MaNhanVien,dt.MaDoiTuong), dt.MaDoiTuong) as MaDoiTuong,
    	iif(hd.LoaiHoaDon = 13, iif(hd.ID_DoiTuong='00000000-0000-0000-0000-000000000002',nv.TenNhanVien,dt.TenDoiTuong), dt.TenDoiTuong) as TenDoiTuong,		
    	isnull(dt.DienThoai,'') as DienThoai,
    	isnull(dt.TenDoiTuong_KhongDau,'') as TenDoiTuong_KhongDau,
    	isnull(nv.MaNhanVien,'') as MaNhanVien,	
    	isnull(nv.TenNhanVien,'') as TenNhanVien,	
    	isnull(nv.TenNhanVienKhongDau,'') as TenNhanVienKhongDau,
		po.MaHoaDon as MaHoaDonGoc,
		isnull(po.LoaiHoaDon,0) as LoaiHoaDonGoc,
		hd.YeuCau,
		case hd.LoaiHoaDon
			when 4 then N'Nhập hàng'
			when 13 then N'Nhập kho nội bộ'
			when 14 then N'Nhập hàng khách thừa'
			when 31 then N'Đặt hàng nhà cung cấp'
		else 'Nhập hàng' end as strLoaiHoaDon,
		case hd.ChoThanhToan			
				when 1 then N'Tạm lưu' 
				when 0 then 
					case hd.YeuCau
						when '1' then  N'Đã lưu' 
						when '2' then  N'Đang xử lý' 
						when '3' then  N'Hoàn thành' 
						when '4' then  N'Đã hủy' 
						else iif(hd.LoaiHoaDon = 31, N'Đã lưu' , N'Đã nhập hàng') end
				else  N'Đã hủy'
				end as TrangThai,
		case hd.ChoThanhToan			
			when 1 then N'0' 
			when 0 then 
				case hd.YeuCau
					when '1' then '1'
					when '2' then '2'
					when '3' then '3'
					when '4' then '4'
					else '1' end
			else '4' end as TrangThaiHD				    	
    	from BH_HoaDon hd
    	join DM_DonVi dv on hd.ID_DonVi= dv.ID
		left join BH_HoaDon po on hd.ID_HoaDon= po.ID
    	left join  DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
		left join NS_NhanVien nv on hd.ID_NhanVien= nv.ID
		left join (
			select 
				ct.ID_HoaDon,
				sum(ct.SoLuong * ct.TienChietKhau) as GiamGiaCT,			
				sum(ct.SoLuong * ct.DonGia) as ThanhTienChuaCK
			from BH_HoaDon_ChiTiet ct
    		join BH_HoaDon hd on ct.ID_HoaDon= hd.ID   			
    		where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
    		and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    		and  exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
			group by ct.ID_HoaDon
		) ctHD on ctHD.ID_HoaDon = hd.ID
    	left join
    	(
				select 
					tblTongChi.ID_HoaDonLienQuan,
					sum(tblTongChi.TienThu) as TienThu,
					sum(tblTongChi.TienMat) as TienMat,
					sum(tblTongChi.TienATM) as TienATM,				
					sum(tblTongChi.ChuyenKhoan) as ChuyenKhoan,
					sum(tblTongChi.TienDatCoc) as TienDatCoc
				from
				(
    				select a.ID_HoaDonLienQuan, 
						sum(TienThu) as TienThu,
						sum(a.TienMat) as TienMat,
						sum(a.TienATM) as TienATM,				
						sum(a.ChuyenKhoan) as ChuyenKhoan,
						sum(a.TienDatCoc) as TienDatCoc
    				from(
    					select qct.ID_HoaDonLienQuan,   
						iif(qct.HinhThucThanhToan =1, qct.TienThu, 0) as TienMat,
						iif(qct.HinhThucThanhToan = 2, qct.TienThu,0) as TienATM,
						iif(qct.HinhThucThanhToan = 3, qct.TienThu,0) as ChuyenKhoan,
						iif(qct.HinhThucThanhToan = 6, qct.TienThu,0) as TienDatCoc,
						iif(qhd.LoaiHoaDon = 11,-qct.TienThu, qct.TienThu) as TienThu
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					join BH_HoaDon hd on qct.ID_HoaDonLienQuan= hd.ID
    					where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
    					and (qhd.TrangThai= 1 or qhd.TrangThai is null)
    					and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    					and  exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
    					) a group by a.ID_HoaDonLienQuan

						Union all
						---- get TongChi from PO: chi get hdXuly first
    					select 
							ID,
							TienThu,
							TienMat, 
							TienATM,
							ChuyenKhoan,
							TienDatCoc
						from
						(	
								Select 
										ROW_NUMBER() OVER(PARTITION BY ID_HoaDonLienQuan ORDER BY NgayLapHoaDon ASC) AS isFirst,						
    									d.ID,
										ID_HoaDonLienQuan,
										d.NgayLapHoaDon,
    									sum(d.TienMat) as TienMat,
    									SUM(ISNULL(d.TienATM, 0)) as TienATM,
    									SUM(ISNULL(d.TienCK, 0)) as ChuyenKhoan,
										SUM(ISNULL(d.TienDoiDiem, 0)) as TienDoiDiem,
										sum(d.ThuTuThe) as ThuTuThe,
    									sum(d.TienThu) as TienThu,
										sum(d.TienDatCoc) as TienDatCoc
									
    								FROM
    								(									
											select hd.ID, hd.NgayLapHoaDon,
												qct.ID_HoaDonLienQuan,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0), iif(qct.HinhThucThanhToan=1, -qct.TienThu, 0)) as TienMat,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0), iif(qct.HinhThucThanhToan=2, -qct.TienThu, 0)) as TienATM,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0), iif(qct.HinhThucThanhToan=3, -qct.TienThu, 0)) as TienCK,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0), iif(qct.HinhThucThanhToan=5, -qct.TienThu, 0)) as TienDoiDiem,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0), iif(qct.HinhThucThanhToan=4, -qct.TienThu, 0)) as ThuTuThe,
												iif(qhd.LoaiHoaDon = 12, qct.TienThu, -qct.TienThu) as TienThu,
												iif(qct.HinhThucThanhToan=6,qct.TienThu,0) as TienDatCoc
											from Quy_HoaDon_ChiTiet qct
											join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
											join BH_HoaDon hdd on hdd.ID= qct.ID_HoaDonLienQuan
											left join BH_HoaDon hd on hd.ID_HoaDon= hdd.ID
											where hdd.LoaiHoaDon = 31	
											and hd.ChoThanhToan = 0
											and (qhd.TrangThai= 1 Or qhd.TrangThai is null)
    								) d group by d.ID,d.NgayLapHoaDon,ID_HoaDonLienQuan						
						) thuDH
						where isFirst= 1
					)tblTongChi
					group by tblTongChi.ID_HoaDonLienQuan
    	) quy on hd.id = quy.ID_HoaDonLienQuan
    	where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
		and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    	and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
    ) hdQuy
    where 
    exists (select ID from dbo.splitstring(@TrangThais) tt where hdQuy.TrangThaiHD= tt.Name)	
    	and
    	((select count(Name) from @tblSearch b where     			
    		hdQuy.MaHoaDon like '%'+b.Name+'%'
    		or hdQuy.NguoiTao like '%'+b.Name+'%'				
    		or hdQuy.TenNhanVien like '%'+b.Name+'%'
    		or hdQuy.TenNhanVienKhongDau like '%'+b.Name+'%'
    		or hdQuy.DienGiai like '%'+b.Name+'%'
    		or hdQuy.MaDoiTuong like '%'+b.Name+'%'		
    		or hdQuy.TenDoiTuong like '%'+b.Name+'%'
    		or hdQuy.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		or hdQuy.DienThoai like '%'+b.Name+'%'		
    		)=@count or @count=0)	
    		),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
					sum(ThanhTienChuaCK) as SumThanhTienChuaCK,
					sum(GiamGiaCT) as SumGiamGiaCT,
    				sum(TongTienHang) as SumTongTienHang,
    				sum(TongGiamGia) as SumTongGiamGia,
					sum(TienMat) as SumTienMat,	
					sum(TienATM) as SumPOS,	
					sum(ChuyenKhoan) as SumChuyenKhoan,	
					sum(TienDatCoc) as SumTienCoc,	
    				sum(KhachDaTra) as SumDaThanhToan,				
    				sum(TongChiPhi) as SumTongChiPhi,
    				sum(PhaiThanhToan) as SumPhaiThanhToan,
    				sum(TongThanhToan) as SumTongThanhToan,				
    				sum(TongTienThue) as SumTongTienThue,
    				sum(ConNo) as SumConNo
    			from data_cte
    		)
    		select dt.*, cte.*	
    		from data_cte dt
    		cross join count_cte cte
    		order by 
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end DESC,
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='ConNo' then ConNo end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='ConNo' then ConNo end DESC,
    			case when @SortBy <>'ASC' then ''
    			when @ColumnSort='MaHoaDon' then MaHoaDon end ASC,
    			case when @SortBy <>'DESC' then ''
    			when @ColumnSort='MaHoaDon' then MaHoaDon end DESC,
    			case when @SortBy <>'ASC' then ''
    			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end ASC,
    			case when @SortBy <>'DESC' then ''
    			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end DESC,
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='TongTienHang' then TongTienHang end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='TongTienHang' then TongTienHang end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='GiamGia' then TongGiamGia end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='GiamGia' then TongGiamGia end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='KhachDaTra' then KhachDaTra end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='KhachDaTra' then KhachDaTra end DESC			
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListCashFlow_Before]
    @IDDonVis [nvarchar](max),
    @ID_NhanVien [nvarchar](40),
    @ID_TaiKhoanNganHang [nvarchar](40),
    @ID_KhoanThuChi [nvarchar](40),
    @DateFrom [datetime],
    @DateTo [datetime],
    @LoaiSoQuy varchar(15),
    @LoaiChungTu [nvarchar](2),
    @TrangThaiSoQuy [nvarchar](2),
    @TrangThaiHachToan [nvarchar](2),
    @TxtSearch [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;

	declare @tblDonVi table (ID_DonVi uniqueidentifier)
	insert into @tblDonVi
	select name from dbo.splitstring(@IDDonVis)

	declare @tblLoai table (Loai int)
	insert into @tblLoai
	select name from dbo.splitstring(@LoaiSoQuy)

	--declare @nguoitao nvarchar(100) = (select taiKhoan from HT_NguoiDung where ID_NhanVien= @ID_NhanVienLogin)
	--declare @tblNhanVien table (ID uniqueidentifier)
	--insert into @tblNhanVien
	--select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @IDDonVis,'SoQuy_XemDS_PhongBan','SoQuy_XemDS_HeThong');
 
 select 
	ceiling(sum(ThuMat)- sum(ChiMat)) as TongThuMat,  
	ceiling(sum(ThuGui) - sum(ChiGui)) as TongThuCK
 from
 (
 select
	iif(a1.LoaiHoaDon=11, TienMat,0) as ThuMat,
	iif(a1.LoaiHoaDon=12, TienMat,0) as ChiMat,
	iif(a1.LoaiHoaDon=11, TienGui,0) as ThuGui,
	iif(a1.LoaiHoaDon=12, TienGui,0) as ChiGui
 from
 (
    	 select
			tblView.LoaiHoaDon,
			sum(TienMat) as TienMat,
			sum(TienGui) as TienGui
	from
		(select 
			tblQuy.MaHoaDon,		
			tblQuy.LoaiHoaDon,
			ISNUll(tblQuy.TrangThai,'1') as TrangThai,
			tblQuy.NoiDungThu,
			tblQuy.ID_NhanVienPT as ID_NhanVien,			
			TienMat, TienGui, TienMat + TienGui as TienThu,
			TienMat + TienGui as TongTienThu,
			cast(ID_TaiKhoanNganHang as varchar(max)) as ID_TaiKhoanNganHang,
			case when tblQuy.LoaiHoaDon = 11 then '11' else '12' end as LoaiChungTu,
    		case when tblQuy.HachToanKinhDoanh = '1' then '11' else '10' end as TrangThaiHachToan,
    		case when tblQuy.TrangThai = '0' then '10' else '11' end as TrangThaiSoQuy,
			case when tblQuy.TienMat > 0 then case when tblQuy.TienGui > 0 then '2' else '1' end 
			else case when tblQuy.TienGui > 0 then '0'
				else case when ID_TaiKhoanNganHang!='00000000-0000-0000-0000-000000000000' then '0' else '1' end end end as LoaiSoQuy
							
		from
			(select 
				 a.ID_hoaDon, 
				 a.MaHoaDon,			
				 a.LoaiHoaDon,
				 a.HachToanKinhDoanh, 
				 a.NoiDungThu,
				 a.ID_NhanVienPT, a.TrangThai,
				 sum(isnull(a.TienMat, 0)) as TienMat,
				 sum(isnull(a.TienGui, 0)) as TienGui,
				 max(a.ID_DoiTuong) as ID_DoiTuong,
				 max(a.ID_NhanVien) as ID_NhanVien,
				 max(a.ID_TaiKhoanNganHang) as ID_TaiKhoanNganHang			
			from
			(				
					select qhd.MaHoaDon,				
					qhd.LoaiHoaDon,
					qhd.HachToanKinhDoanh, qhd.PhieuDieuChinhCongNo, qhd.NoiDungThu,
					qhd.ID_NhanVien as ID_NhanVienPT, qhd.TrangThai,
					qct.ID_HoaDon, 
					iif(qct.HinhThucThanhToan= 1, qct.TienThu,0) as TienMat,
					iif(qct.HinhThucThanhToan= 2 or qct.HinhThucThanhToan = 3, qct.TienThu,0) as TienGui,		
					qct.ID_DoiTuong, qct.ID_NhanVien, 
					ISNULL(qct.ID_TaiKhoanNganHang,'00000000-0000-0000-0000-000000000000') as ID_TaiKhoanNganHang,
					ISNULL(qct.ID_KhoanThuChi,'00000000-0000-0000-0000-000000000000') as ID_KhoanThuChi
					from Quy_HoaDon_ChiTiet qct
					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
					join @tblDonVi cn on qhd.ID_DonVi= cn.ID_DonVi
					left join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang= tk.ID
					left join Quy_KhoanThuChi ktc on qct.ID_KhoanThuChi= ktc.ID
					where qhd.NgayLapHoaDon < @DateFrom
					and qct.HinhThucThanhToan not in (4,5,6)
					and (qhd.PhieuDieuChinhCongNo !='1' or qhd.PhieuDieuChinhCongNo is null)
					and (@ID_TaiKhoanNganHang  ='%%' Or qct.ID_TaiKhoanNganHang like @ID_TaiKhoanNganHang)
					and (@ID_KhoanThuChi ='%%' or qct.ID_KhoanThuChi like @ID_KhoanThuChi)				
			) a
			 group by a.ID_HoaDon, a.MaHoaDon, 
				a.LoaiHoaDon,
				a.HachToanKinhDoanh, a.PhieuDieuChinhCongNo, a.NoiDungThu,
				a.ID_NhanVienPT , a.TrangThai
		) tblQuy
		--left join DM_DoiTuong dt on tblQuy.ID_DoiTuong = dt.ID
	 ) tblView
	 where tblView.TrangThaiHachToan like '%'+ @TrangThaiHachToan + '%'
	-- and tblView.MaHoaDon not like 'CB%'		
    	and tblView.TrangThaiSoQuy like '%'+ @TrangThaiSoQuy + '%'
    	and tblView.LoaiChungTu like '%'+ @LoaiChungTu + '%'
    	--and ID_NhanVien like @ID_NhanVien
		and exists (select Loai from @tblLoai loai where LoaiSoQuy= loai.Loai)    
		group by tblView.LoaiHoaDon
		) a1
		) b
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListCashFlow_Paging]
	@IDDonVis [nvarchar](max),
    @ID_NhanVien [nvarchar](40),
    @ID_NhanVienLogin [uniqueidentifier],
    @ID_TaiKhoanNganHang [nvarchar](40),
    @ID_KhoanThuChi [nvarchar](40),
    @DateFrom [datetime],
    @DateTo [datetime],
    @LoaiSoQuy [nvarchar](15),	-- mat/nganhang/all
    @LoaiChungTu [nvarchar](2), -- thu/chi
    @TrangThaiSoQuy [nvarchar](2),
    @TrangThaiHachToan [nvarchar](2),
    @TxtSearch [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int],
	@LoaiNapTien [nvarchar](15) -- 11.tiencoc, 10. khongphai tiencoc, 1.all
AS
BEGIN

	SET NOCOUNT ON;

	set @DateTo = DATEADD(MILLISECOND,-2,@DateTo) ---- tranh truong hop 00.00.00

	declare @isNullSearch int = 1
	if isnull(@TxtSearch,'')='' OR @TxtSearch ='%%'
		begin
			set @isNullSearch =0 
			set @TxtSearch ='%%'
		end
	else
		set @TxtSearch= CONCAT(N'%',@TxtSearch, '%')

	declare @tblChiNhanh table (ID uniqueidentifier)
    insert into @tblChiNhanh
	select name from dbo.splitstring(@IDDonVis)


	--insert into #tblQuyHD
	select qhd.ID,
		qhd.MaHoaDon, qhd.NgayLapHoaDon, qhd.ID_DonVi, qhd.LoaiHoaDon, qhd.NguoiTao,
    	qhd.HachToanKinhDoanh, qhd.PhieuDieuChinhCongNo, qhd.NoiDungThu,
    	qhd.ID_NhanVien as ID_NhanVienPT, qhd.TrangThai	
	into #tblQuyHD
	from Quy_HoaDon qhd	
	where qhd.NgayLapHoaDon between  @DateFrom and  @DateTo		
	and qhd.ID_DonVi in (select * from dbo.splitstring(@IDDonVis))
	and(qhd.PhieuDieuChinhCongNo != '1' or qhd.PhieuDieuChinhCongNo is null)


    	declare @nguoitao nvarchar(100) = (select taiKhoan from HT_NguoiDung where ID_NhanVien= @ID_NhanVienLogin)
    	declare @tblNhanVien table (ID uniqueidentifier)
    	insert into @tblNhanVien
    	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @IDDonVis,'SoQuy_XemDS_PhongBan','SoQuy_XemDS_HeThong');
    	
    	with data_cte
    	as(

    select tblView.*
    	from
    		(
			select 
    			tblQuy.ID,
    			tblQuy.MaHoaDon,
    			tblQuy.NgayLapHoaDon,
    			tblQuy.ID_DonVi,
    			tblQuy.LoaiHoaDon,
    			tblQuy.NguoiTao,
				ISNUll(nv2.TenNhanVien,'') as TenNhanVien,
				ISNUll(nv2.TenNhanVienKhongDau,'') as TenNhanVienKhongDau,
			
				ISNUll(dv.TenDonVi,'') as TenChiNhanh,
				ISNUll(dv.SoDienThoai,'') as DienThoaiChiNhanh,
				ISNUll(dv.DiaChi,'') as DiaChiChiNhanh,
				ISNUll(nguon.TenNguonKhach,'') as TenNguonKhach,
    			ISNUll(tblQuy.TrangThai,'1') as TrangThai,
    			tblQuy.NoiDungThu,
				iif(@isNullSearch=0, dbo.FUNC_ConvertStringToUnsign(NoiDungThu), tblQuy.NoiDungThu) as NoiDungThuUnsign,
    			tblQuy.PhieuDieuChinhCongNo,
    			tblQuy.ID_NhanVienPT as ID_NhanVien,
    			iif(LoaiHoaDon=11, TienMat,0) as ThuMat,
    			iif(LoaiHoaDon=12, TienMat,0) as ChiMat,
    			iif(LoaiHoaDon=11, TienGui,0) as ThuGui,
    			iif(LoaiHoaDon=12, TienGui,0) as ChiGui,
    			TienMat + TienGui as TienThu,
    			TienMat + TienGui as TongTienThu,
				TienGui,
				TienMat, 
				ChuyenKhoan, 
				TienPOS,
				TienDoiDiem, 
				TTBangTienCoc,
				TienTheGiaTri,
    			TenTaiKhoanPOS, TenTaiKhoanNOTPOS,
    			cast(ID_TaiKhoanNganHang as varchar(max)) as ID_TaiKhoanNganHang,
    			ID_KhoanThuChi,
    			NoiDungThuChi,
				tblQuy.ID_NhanVienPT,
				dt.ID_NguonKhach,
				isnull(dt.LoaiDoiTuong,0) as LoaiDoiTuong,
    			ISNULL(tblQuy.HachToanKinhDoanh,'1') as HachToanKinhDoanh,
    			case when tblQuy.LoaiHoaDon = 11 then '11' else '12' end as LoaiChungTu,
    		case when tblQuy.HachToanKinhDoanh = '1' or tblQuy.HachToanKinhDoanh is null  then '11' else '10' end as TrangThaiHachToan,
    		case when tblQuy.TrangThai = '0' then '10' else '11' end as TrangThaiSoQuy,
    		case when nv.TenNhanVien is null then  dt.TenDoiTuong  else nv.TenNhanVien end as NguoiNopTien,
			case when nv.TenNhanVien is null then  dt.DiaChi  else nv.DiaChiCoQuan end as DiaChiKhachHang,
    		case when nv.TenNhanVien is null then  dt.TenDoiTuong_KhongDau  else nv.TenNhanVienKhongDau end as TenDoiTuong_KhongDau,
    		case when nv.MaNhanVien is null then dt.MaDoiTuong else  nv.MaNhanVien end as MaDoiTuong,
    		case when nv.MaNhanVien is null then dt.DienThoai else  nv.DienThoaiDiDong  end as SoDienThoai,
    			case when qct.TienMat > 0 then case when qct.TienGui > 0 then '2' else '1' end 
    			else case when qct.TienGui > 0 then '0'
    				else case when ID_TaiKhoanNganHang!='00000000-0000-0000-0000-000000000000' then '0' else '1' end end end as LoaiSoQuy,
    			-- check truong hop tongthu = 0
    		case when qct.TienMat > 0 then case when qct.TienGui > 0 then N'Tiền mặt, chuyển khoản' else N'Tiền mặt' end 
    			else case when qct.TienGui > 0 then N'Chuyển khoản' else 
    				case when ID_TaiKhoanNganHang!='00000000-0000-0000-0000-000000000000' then  N'Chuyển khoản' else N'Tiền mặt' end end end as PhuongThuc	
    							
    		from #tblQuyHD tblQuy
			 join 
    			(select 
    				 a.ID_hoaDon,
    				 sum(isnull(a.TienMat, 0)) as TienMat,
    				 sum(isnull(a.TienGui, 0)) as TienGui,
					 sum(isnull(a.TienPOS, 0)) as TienPOS,
					 sum(isnull(a.ChuyenKhoan, 0)) as ChuyenKhoan,
					 sum(isnull(a.TienDoiDiem, 0)) as TienDoiDiem,
					 sum(isnull(a.TienTheGiaTri, 0)) as TienTheGiaTri,
					 sum(isnull(a.TTBangTienCoc, 0)) as TTBangTienCoc,
    				 max(a.TenTaiKhoanPOS) as TenTaiKhoanPOS,
    				 max(a.TenTaiKhoanNOPOS) as TenTaiKhoanNOTPOS,
    				 max(a.ID_DoiTuong) as ID_DoiTuong,
    				 max(a.ID_NhanVien) as ID_NhanVien,
    				 max(a.ID_TaiKhoanNganHang) as ID_TaiKhoanNganHang,
    				 max(a.ID_KhoanThuChi) as ID_KhoanThuChi,
    				 max(a.NoiDungThuChi) as NoiDungThuChi
    			from
    			(
    				select *
    				from(
    					select 
    					qct.ID_HoaDon,
						iif(qct.HinhThucThanhToan= 1, qct.TienThu,0) as TienMat,
						iif(qct.HinhThucThanhToan= 2 or qct.HinhThucThanhToan = 3, qct.TienThu,0) as TienGui,			
						iif(qct.HinhThucThanhToan= 2, qct.TienThu,0) as TienPOS,
						iif(qct.HinhThucThanhToan= 3, qct.TienThu,0) as ChuyenKhoan,
						iif(qct.HinhThucThanhToan= 4, qct.TienThu,0) as TienDoiDiem,
						iif(qct.HinhThucThanhToan= 5, qct.TienThu,0) as TienTheGiaTri,
						iif(qct.HinhThucThanhToan= 6, qct.TienThu,0) as TTBangTienCoc,						
						qct.ID_DoiTuong, qct.ID_NhanVien, 
    					ISNULL(qct.ID_TaiKhoanNganHang,'00000000-0000-0000-0000-000000000000') as ID_TaiKhoanNganHang,
    					ISNULL(qct.ID_KhoanThuChi,'00000000-0000-0000-0000-000000000000') as ID_KhoanThuChi,
    					case when tk.TaiKhoanPOS='1' then IIF(tk.TrangThai = 0, '<span style=""color: red; text - decoration: line - through; "" title=""Đã xóa"">' + tk.TenChuThe + '</span>', tk.TenChuThe) else '' end as TenTaiKhoanPOS,
    					case when tk.TaiKhoanPOS = '0' then IIF(tk.TrangThai = 0, '<span style=""color:red;text-decoration: line-through;"" title=""Đã xóa"">' + tk.TenChuThe + '</span>', tk.TenChuThe) else '' end as TenTaiKhoanNOPOS,
    					iif(ktc.NoiDungThuChi is null, '',
						iif(ktc.TrangThai = 0, concat(ktc.NoiDungThuChi, '{DEL}'), ktc.NoiDungThuChi)) as NoiDungThuChi,
						----11.coc, 13.khongbutru congno, 10.khong coc


						iif(qct.LoaiThanhToan = 1, '11', iif(qct.LoaiThanhToan = 3, '13', '10')) as LaTienCoc, 
						IIF(qct.ID_HoaDonLienQuan IS NULL AND qct.ID_KhoanThuChi IS NULL, 1, 0) AS LaThuChiMacDinh


						from #tblQuyHD  qhd		
						left join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDon = qhd.ID


						left
						join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang = tk.ID



				   left
						join Quy_KhoanThuChi ktc on qct.ID_KhoanThuChi = ktc.ID


						where qct.HinhThucThanhToan not in (4, 5, 6)-- diem, thegiatri, coc
    					)qct
					where qct.ID_TaiKhoanNganHang like @ID_TaiKhoanNganHang


					and(qct.ID_KhoanThuChi like @ID_KhoanThuChi OR(qct.LaThuChiMacDinh = 1 AND @ID_KhoanThuChi = '00000000-0000-0000-0000-000000000001'))


					and qct.LaTienCoc like '%' + @LoaiNapTien + '%'
    			) a group by a.ID_HoaDon
    		) qct on tblQuy.ID = qct.ID_HoaDon


			left join DM_DoiTuong dt on qct.ID_DoiTuong = dt.ID


			left join NS_NhanVien nv on qct.ID_NhanVien = nv.ID


		left join DM_DonVi dv on tblQuy.ID_DonVi = dv.ID


		left join NS_NhanVien nv2 on tblQuy.ID_NhanVienPT = nv2.ID


		left join DM_NguonKhachHang nguon on dt.ID_NguonKhach = nguon.ID
    	 ) tblView

		 where tblView.TrangThaiHachToan like '%' + @TrangThaiHachToan + '%'



		and tblView.TrangThaiSoQuy like '%' + @TrangThaiSoQuy + '%'



		and tblView.LoaiChungTu like '%' + @LoaiChungTu + '%'




			and tblView.ID_NhanVienPT like @ID_NhanVien



			and(exists(select ID from @tblNhanVien nv where tblView.ID_NhanVienPT = nv.ID) or tblView.NguoiTao like @nguoitao)



		and exists(select Name from dbo.splitstring(@LoaiSoQuy) where LoaiSoQuy= Name)


			and(MaHoaDon like @TxtSearch


			OR MaDoiTuong like @TxtSearch


			OR NguoiNopTien like @TxtSearch


			OR SoDienThoai like @TxtSearch


			OR TenNhanVien like @TxtSearch--nvlap


			OR TenNhanVienKhongDau like @TxtSearch


			OR TenDoiTuong_KhongDau like @TxtSearch-- nguoinoptien


			OR NoiDungThuUnsign like @TxtSearch
			)
    	),
    	count_cte
		as (
		select count(dt.ID) as TotalRow,
    		CEILING(count(dt.ID) / cast(@PageSize as float)) as TotalPage,
    		sum(ThuMat) as TongThuMat,
    		sum(ChiMat) as TongChiMat,
    		sum(ThuGui) as TongThuCK,
    		sum(ChiGui) as TongChiCK



		from data_cte dt
    	)
    	select*
		from data_cte dt



		cross join count_cte
		order by dt.NgayLapHoaDon desc



		OFFSET(@CurrentPage * @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY
END
");

			Sql(@"ALTER PROCEDURE [dbo].[GetListLichHen_FullCalendar]
    @IDChiNhanhs [nvarchar](max),
    @IDLoaiTuVans [nvarchar](max),
    @IDNhanVienPhuTrachs [nvarchar](max),
    @TrangThaiCVs [nvarchar](20),
    @PhanLoai [nvarchar](20),
    @DoUuTien [nvarchar](4),
    @LoaiDoiTuong [nvarchar](10),
    @FromDate [datetime],
    @ToDate [datetime],
    @IDKhachHang [nvarchar](40),
	@TextSearch nvarchar(max),
	@CurrentPage int,
	@PageSize int
AS
BEGIN
    SET NOCOUNT ON;

	set @FromDate = DATEADD(SECOND, -1, @FromDate)

	declare @today datetime = format( DATEADD(SECOND, -1, getdate()),'yyyy-MM-dd')
	declare @SFromDate nvarchar(max) = CONVERT(varchar,  DATEADD(SECOND, -1, @FromDate),23)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);

	declare @tblChiNhanh table(ID uniqueidentifier)
	insert into @tblChiNhanh
	select Name from dbo.splitstring(@IDChiNhanhs)

	declare @tblTrangThai table(TrangThaiCV int)
	insert into @tblTrangThai
	select Name from dbo.splitstring(@TrangThaiCVs)

	CREATE TABLE #temp(	
		ID uniqueidentifier not null, 
    	Ma_TieuDe nvarchar(max),
    	ID_DonVi uniqueidentifier, 
    	ID_KhachHang uniqueidentifier, 
    	ID_LoaiTuVan uniqueidentifier,
    	ID_NhanVien uniqueidentifier,	
    	ID_NhanVienQuanLy uniqueidentifier,
    	NgayTao datetime,
    	NgayGio datetime,
    	NgayGioKetThuc datetime,	
    	NgayHoanThanh datetime,
		KieuLap int ,
		SoLanLap int,
		GiaTriLap varchar(max),
		TuanLap int,
		TrangThaiKetThuc int,
		GiaTriKetThuc varchar(max),
		SoLanDaHen int,
		TrangThai int,
		GhiChu nvarchar(max),
		NoiDung nvarchar(max),
		NguoiTao nvarchar(max),
		MucDoUuTien int,
		KetQua nvarchar(max),
		NhacNho int,
		KieuNhacNho int,
		ID_Parent uniqueidentifier,
		NgayCu datetime   	
)

CREATE TABLE #tempCur(	
		ID uniqueidentifier not null, 
    	Ma_TieuDe nvarchar(max),
    	ID_DonVi uniqueidentifier, 
    	ID_KhachHang uniqueidentifier, 
    	ID_LoaiTuVan uniqueidentifier,
    	ID_NhanVien uniqueidentifier,	
    	ID_NhanVienQuanLy uniqueidentifier,
    	NgayTao datetime,
    	NgayGio datetime,
    	NgayGioKetThuc datetime,	
    	NgayHoanThanh datetime,
		KieuLap int not null,
		SoLanLap int,
		GiaTriLap varchar(max),
		TuanLap int,
		TrangThaiKetThuc int,
		GiaTriKetThuc varchar(max),
		SoLanDaHen int,
		TrangThai int,
		GhiChu nvarchar(max),
		NoiDung nvarchar(max),
		NguoiTao nvarchar(max),
		MucDoUuTien int,
		KetQua nvarchar(max),
		NhacNho int,
		KieuNhacNho int,
		ID_Parent uniqueidentifier,
		NgayCu datetime   	
)

CREATE CLUSTERED INDEX calendar_KieLap ON #tempCur(KieuLap)

CREATE TABLE #temp2(	
		ID uniqueidentifier PRIMARY KEY,     	
		ID_Parent uniqueidentifier not null,
		NgayCu datetime   	,
		NgayCu_yyyyMMdd datetime
)
 

	declare @tblNVPhuTrach table(ID uniqueidentifier)
	insert into @tblNVPhuTrach
	select Name from dbo.splitstring(@IDNhanVienPhuTrachs)

	declare @tblLoaiCV table(ID uniqueidentifier)
	insert into @tblLoaiCV
	select Name from dbo.splitstring(@IDLoaiTuVans)
	
    
    declare @tblCalendar table(ID uniqueidentifier,Ma_TieuDe nvarchar (max), ID_DonVi uniqueidentifier, ID_KhachHang uniqueidentifier,ID_LoaiTuVan uniqueidentifier null,
    	ID_NhanVien uniqueidentifier, ID_NhanVienQuanLy uniqueidentifier null, 
    	NgayTao datetime,NgayHenGap datetime, NgayGioKetThuc datetime null,NgayHoanThanh datetime null,
    	TrangThai varchar(10), GhiChu nvarchar(max), NoiDung nvarchar(max), NguoiTao nvarchar(max),
    	MucDoUuTien int, KetQua nvarchar(max), NhacNho int null, KieuNhacNho int null,
    	KieuLap int null, SoLanLap int null, GiaTriLap nvarchar(max) null,TuanLap int null, TrangThaiKetThuc int null,GiaTriKetThuc nvarchar(max), 
    	ExistDB bit,ID_Parent uniqueidentifier null, NgayCu datetime null)	
    
	insert into #temp WITH(TABLOCKX)
    --- table LichHen
    select cs.ID, 
    	Ma_TieuDe,
    	cs.ID_DonVi, 
    	ID_KhachHang, 
    	ID_LoaiTuVan,
    	ID_NhanVien,	
    	ID_NhanVienQuanLy,
    	NgayTao,
    	NgayGio,
    	NgayGioKetThuc,	
    	NgayHoanThanh,
    	ISNULL(KieuLap,0) as KieuLap,
    	ISNULL(SoLanLap,0) as SoLanLap,
    	ISNULL(GiaTriLap,'') as GiaTriLap,
    	ISNULL(TuanLap,0) as TuanLap,
    	ISNULL(TrangThaiKetThuc,0) as TrangThaiKetThuc,
    	ISNULL(GiaTriKetThuc,'') as GiaTriKetThuc, 	
    	ISNULL(a.SoLanDaHen,0) as SoLanDaHen,
    	TrangThai,
		ISNULL(GhiChu,'') as GhiChu,
    	ISNULL(NoiDung,'') as NoiDung,
    	NguoiTao,
    	2 as MucDoUuTien,
    	KetQua,
    	NhacNho, 
    	ISNULL(KieuNhacNho,0) as KieuNhacNho,
		case when cs.ID_Parent is null then cs.ID else cs.ID_Parent end as ID_Parent,
    	cs.NgayCu
    from ChamSocKhachHangs cs
    left join  
		( select ISNULL(ID_Parent,'00000000-0000-0000-0000-000000000000') as ID_Parent,
    		count(ID) as SoLanDaHen
    		from ChamSocKhachHangs
    		where PhanLoai = 3
    		group by ID_Parent) a 
			on cs.ID= a.ID_Parent
    where KieuLap is not null
    	and (TrangThaiKetThuc = 1 
    	OR (TrangThaiKetThuc = 2 and ISNULL(GiaTriKetThuc,'')  > @SFromDate)
    	OR (TrangThaiKetThuc = 3 and ISNULL(a.SoLanDaHen,0)  < ISNULL(GiaTriKetThuc,1)) 
		OR TrangThaiKetThuc is null
    	)	
    and PhanLoai = 3 
	and exists (select ID from @tblChiNhanh cn where cn.id = cs.ID_DonVi)
    
    -- get row was update (ID_Parent !=null)
	insert into #temp2  WITH(TABLOCKX)
    select ID, ID_Parent, NgayCu, format(NgayCu,'yyyy-MM-dd') as NgayCu_yyyyMMdd 
	from #temp where ID_Parent != ID

	insert into #tempCur WITH(TABLOCKX)
	select t1.* 
	from #temp t1
	left join #temp2 t2 on t1.ID= t2.ID
	where t1.SoLanLap > 0 and t1.TrangThai = 1 ---- trangthai = 1. Dang xu ly <=> chưa hẹn gặp khách
	and t2.ID is null
   
    declare @ID uniqueidentifier, @Ma_TieuDe nvarchar(max), @ID_DonVi uniqueidentifier, @ID_KhachHang uniqueidentifier,@ID_LoaiTuVan uniqueidentifier, 
    		@ID_NhanVien uniqueidentifier,@ID_NhanVienQuanLy uniqueidentifier,
    		@NgayTao datetime,@NgayGio datetime,@NgayGioKetThuc datetime, @NgayHoanThanh datetime,
    		@KieuLap int, @SoLanLap int, @GiaTriLap varchar(max), @TuanLap int, @TrangThaiKetThuc int,@GiaTriKetThuc varchar(max),			
    		@SoLanDaHen int, @TrangThai varchar, @GhiChu nvarchar(max),@NoiDung nvarchar(max),
    		@NguoiTao nvarchar(max), @MucDoUuTien int, @KetQua nvarchar(max), @NhacNho int, @KieuNhacNho int, @ID_Parent uniqueidentifier, @NgayCu datetime
    
    		--- lap ngay
    		declare _cur cursor
    		for
    			select * 
				from #tempCur t1				
				where t1.KieuLap = 1 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc, @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc, @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				-- chi add row < @ToDate
    				declare @dateadd datetime = @NgayGio

					---- tính số ngày bắt đầu từ @NgayGio in DB - đến @FromDate								
					if  @TrangThaiKetThuc = 1 
						begin
							declare @totalDay int = datediff(day,@dateadd, @FromDate)							
							if @totalDay > 0
								set @dateadd = DATEADD(day, @totalDay, @dateadd)
						end
				
    				declare @lanlap int = 1			
    				while @dateadd < @ToDate 
    					begin	
    						declare @dateadd_yyyyMMdd datetime= format(@dateadd,'yyyy-MM-dd')
    						if @TrangThaiKetThuc= 1 
    							OR (@TrangThaiKetThuc = 2 and  @dateadd < @GiaTriKetThuc )  --- khong bao gio OR KetThuc vao ngay OR sau x lan (todo)
    							OR (@TrangThaiKetThuc= 3 and @lanlap <= @GiaTriKetThuc - @SoLanDaHen)
    							begin
    								set @NgayGioKetThuc = DATEADD(hour,1,@dateadd)
    								declare @newidDay uniqueidentifier = NEWID()
    								declare @count1 int = 0;
    								if @dateadd = @NgayGio set @newidDay = @ID		
    								select @count1 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    									and NgayCu_yyyyMMdd = @dateadd_yyyyMMdd								
    								if @count1 = 0		
										begin
    										insert into @tblCalendar values (@newidDay,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    										@NgayTao, @dateadd,@NgayGioKetThuc, @NgayHoanThanh, @TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho,
    										@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc, IIF(@dateadd = @NgayGio,'1','0'),@ID_Parent, @NgayCu)																			
											set @lanlap= @lanlap + 1
										end
    							end
    						set @dateadd = DATEADD(day, @SoLanLap, @dateadd)
    					end
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap,@TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho,@ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;
    
    		--- lap tuan
    		declare _cur2 cursor
    		for
				select * 
				from #tempCur t1				
				where t1.KieuLap = 2 
    		open _cur2
    		fetch next from _cur2
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin	
    				declare @weekRepeat datetime = @NgayGio			
					declare @hour int= datepart(hour, @NgayGio)
					declare @minute int= datepart(minute, @NgayGio)
    				declare @lanlapWeek int = 1

					---- tính số tuần bắt đầu từ @NgayGio in DB - đến @FromDate			
					if  @TrangThaiKetThuc = 1 -- khong bao gio ket thuc
						begin							
							declare @totalWeek int = datediff(week,@weekRepeat, @FromDate)
							if @totalWeek > 0
								set @weekRepeat = DATEADD(WEEK, @totalWeek, @weekRepeat)
						end

    				while @weekRepeat < @ToDate -- lặp đến khi thuộc khoảng thời gian tìm kiếm
    					begin	
    								declare @firstOfWeek datetime = (select  dateadd(WEEK, datediff(WEEK, 0, @weekRepeat), 0)) -- lay ngay dau tien cua tuan
    								declare @lastOfWeek datetime = (select  dateadd(WEEK, datediff(WEEK, 0, @weekRepeat), 7)) -- lay ngay cuoi cung cua tuan
    								declare @dateRepeat datetime = @firstOfWeek	
									

    								while @dateRepeat < @lastOfWeek -- tim kiem trong tuan duoc lap lai
    									begin
    										if dateadd(hour,23, @dateRepeat) >= @NgayGio
    											begin
    												declare @dateOfWeek varchar(1) = cast(DATEPART(WEEKDAY,@dateRepeat) as varchar(1)) -- lấy ngày trong tuần (thứ 2,3,..)	
    												if @dateOfWeek = 1 set @dateOfWeek = 8
    												declare @datefrom datetime = dateadd(minute, @minute, dateadd(hour,@hour, @dateRepeat))
    												set @NgayGioKetThuc = DATEADD(hour,1,@datefrom) -- add 2 hour
													declare @dateRepeat_yyyyMMdd datetime= format(@dateRepeat,'yyyy-MM-dd')
    
    												if CHARINDEX(@dateOfWeek, @GiaTriLap ) > 0 
    												and  (@TrangThaiKetThuc= 1 OR (@TrangThaiKetThuc = 2 and  @dateRepeat <= @GiaTriKetThuc)
    													OR (@TrangThaiKetThuc= 3 and @lanlapWeek <= @GiaTriKetThuc - @SoLanDaHen))
    													begin														
    														declare @newidWeek uniqueidentifier = NEWID()
    														declare @exitDB bit='0'
    														if @dateRepeat_yyyyMMdd = format(@NgayGio,'yyyy-MM-dd') 
    															begin
    																set @newidWeek = @ID
    																set @exitDB ='1'
    															end
    														declare @count2 int=0
    														select @count2 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    																and NgayCu_yyyyMMdd = @dateRepeat_yyyyMMdd								
    														if @count2 = 0	
    															begin
    																insert into @tblCalendar values (@newidWeek,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    																		@NgayTao, @datefrom,@NgayGioKetThuc,  @NgayHoanThanh, 
    																		@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho,
    																		@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc,@exitDB, @ID_Parent, @NgayCu)
    																set @lanlapWeek= @lanlapWeek + 1																
    															end
    													end												
    											end										
    										set @dateRepeat = DATEADD(day, 1, @dateRepeat)											
    									end							
    						set @weekRepeat = DATEADD(WEEK, @SoLanLap, @weekRepeat)	-- lap lai x tuan/lan	
    					end			
    				FETCH NEXT FROM _cur2 into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur2;
    		deallocate _cur2;
    
    		--- lap thang
    		declare _cur cursor
    		for				
				select * 
				from #tempCur t1				
				where t1.KieuLap = 3 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				declare @monthRepeat datetime = @NgayGio	
    				declare @lanlapMonth int = 1

					if  @TrangThaiKetThuc = 1
						begin
							---- tính số ngày bắt đầu từ @NgayGio in DB - đến @FromDate
							declare @totalMonth int = datediff(MONTH,@monthRepeat, @FromDate)
							if @totalMonth > 0
								set @monthRepeat = DATEADD(MONTH, @totalMonth, @monthRepeat)
						end

    				while @monthRepeat < @ToDate -- lặp trong khoảng thời gian tìm kiếm
    					begin	
    						if  @monthRepeat > @FromDate			
    							begin	
    								declare @datefromMonth datetime= @monthRepeat
									declare @monthRepeat_yyyyMMdd datetime= format(@monthRepeat,'yyyy-MM-dd')

    								set @NgayGioKetThuc = DATEADD(hour,1,@datefromMonth)
    								 -- hàng tháng vào ngày ..xx..
    								if	@TuanLap = 0 
    									and (@TrangThaiKetThuc = 1 
    									OR (@TrangThaiKetThuc = 2 and @monthRepeat < @GiaTriKetThuc)
    									OR (@TrangThaiKetThuc= 3 and @lanlapMonth <= @GiaTriKetThuc - @SoLanDaHen)
    									)
    									begin
    											declare @newidMonth1 uniqueidentifier = NEWID()
    											if @monthRepeat = @NgayGio set @newidMonth1 = @ID
    											declare @count3 int=0
    											select @count3 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    													and NgayCu_yyyyMMdd = @monthRepeat_yyyyMMdd								
    											if @count3 = 0	
    											insert into @tblCalendar values (@newidMonth1,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    																@NgayTao, @monthRepeat,@NgayGioKetThuc,  @NgayHoanThanh,
    																@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, 
    																@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc, IIF(@monthRepeat = @NgayGio,'1','0'), @ID_Parent, @NgayCu)
    									end 
    								else
    									-- hàng tháng vào thứ ..x.. tuần thứ ..y.. của tháng
    									begin
    										declare @dateOfWeek_Month int = DATEPART(WEEKDAY,@monthRepeat) -- thu may trong tuan
    										if @dateOfWeek_Month = 8 set @dateOfWeek_Month = 1
    										declare @weekOfMonth int = DATEDIFF(WEEK, DATEADD(MONTH, DATEDIFF(MONTH, 0, @monthRepeat), 0), @monthRepeat) +1 -- tuan thu may cua thang
    										if @dateOfWeek_Month = @GiaTriLap and @weekOfMonth = @TuanLap 
    										and (@TrangThaiKetThuc = 1 
    											OR (@TrangThaiKetThuc = 2 and @monthRepeat < @GiaTriKetThuc)
    											OR (@TrangThaiKetThuc= 3 and @lanlapMonth <= @GiaTriKetThuc - @SoLanDaHen)
    											)
    											begin
    												declare @newidMonth2 uniqueidentifier = NEWID()
    												if @monthRepeat = @NgayGio set @newidMonth2 = @ID
    												declare @count4 int=0
    												select @count4 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    														and NgayCu_yyyyMMdd = @monthRepeat_yyyyMMdd							
    												if @count4 = 0	
    												insert into @tblCalendar values (@newidMonth2,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    																@NgayTao, @monthRepeat,@NgayGioKetThuc,  @NgayHoanThanh,
    																@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, 
    																@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc,IIF(@monthRepeat = @NgayGio,'1','0'), @ID_Parent, @NgayCu)
    											end
    									end						
    							end
    						set @monthRepeat = DATEADD(MONTH, @SoLanLap, @monthRepeat)	-- lap lai x thang/lan	
    						set @lanlapMonth = @lanlapMonth +1
    					end			
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;
    
    
    		--- lap nam
    		declare _cur cursor
    		for
				select * 
				from #tempCur t1				
				where t1.KieuLap = 4 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				declare @yearRepeat datetime = @NgayGio
					declare @yearRepeat_yyyyMMdd datetime= format(@yearRepeat,'yyyy-MM-dd')
    				declare @lanlapYear int = 1
    				while @yearRepeat < @ToDate -- lặp trong khoảng thời gian tìm kiếm
    					begin						
    						if  @yearRepeat > @FromDate			
    							begin	
    								declare @dateOfMonth int = datepart(day,@yearRepeat)
    								declare @monthOfYear int = datepart(MONTH,@yearRepeat)
    								set @NgayGioKetThuc= DATEADD(hour,1, @yearRepeat)
    
    								if @dateOfMonth = @GiaTriLap and @monthOfYear= @TuanLap
    									and (@TrangThaiKetThuc = 1 
    										OR (@TrangThaiKetThuc = 2 and @yearRepeat < @GiaTriKetThuc)
    										OR (@TrangThaiKetThuc = 3 and @lanlapYear <= @GiaTriKetThuc - @SoLanDaHen)
    										)
    									begin
    										declare @newidYear uniqueidentifier = NEWID()										
    										if @yearRepeat = @NgayGio set @newidYear = @ID
    										declare @count5 int=0
    										select @count5 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    												and NgayCu_yyyyMMdd = @yearRepeat_yyyyMMdd							
    										if @count5 = 0	
    										insert into @tblCalendar values (@newidYear,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    															@NgayTao, @yearRepeat,@NgayGioKetThuc, @NgayHoanThanh, 
    															@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, 
    															@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc,IIF(@yearRepeat = @NgayGio,'1','0'), @ID_Parent, @NgayCu)
    									end
    							end
    						set @yearRepeat = DATEADD(YEAR, @SoLanLap, @yearRepeat)	-- lap lai x nam/lan	
    						set @lanlapYear = @lanlapYear +1
    					end			
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;

			--select * from #temp
    	
    	-- add LichHen da duoc update (SoLanLap = 0)
    	insert into @tblCalendar
    	select tbl1.ID, 
    		Ma_TieuDe,
    		ID_DonVi, 
    		ID_KhachHang, 
    		ID_LoaiTuVan,
    		ID_NhanVien,	
    		ID_NhanVienQuanLy,
    		NgayTao,
    		NgayGio,
    		NgayGioKetThuc,
    		NgayHoanThanh,
    		TrangThai,
    		GhiChu,
			NoiDung,
    		NguoiTao,
    		2 as MucDoUuTien, 
    		KetQua,
    		NhacNho,
    		ISNULL(KieuNhacNho,0) as KieuNhacNho,
    		ISNULL(KieuLap,0) as KieuLap,
    		ISNULL(SoLanLap,0) as SoLanLap,
    		ISNULL(GiaTriLap,'') as GiaTriLap,
    		ISNULL(TuanLap,0) as TuanLap,
    		ISNULL(TrangThaiKetThuc,0) as TrangThaiKetThuc,
    		ISNULL(GiaTriKetThuc,'') as GiaTriKetThuc,
    		'1' as ExistDB, 
    		tbl1.ID_Parent,
    		tbl1.NgayCu
    	from #temp tbl1
		left join #temp2 tbl2 on tbl1.ID = tbl2.ID
		where (KieuLap = 0 OR TrangThai !='1' 
    		Or tbl2.ID is not null
    		)
    		and NgayGio between @FromDate and @ToDate;
    			
    	--drop table #temp 
    	--drop table #temp2 ;

		with data_cte
		as 
		(    
    	select b.*, 
			case when PhanLoai= 3 then 'rgb(11, 128, 67)' -- lichhen: xanhla
				else
					case when MucDoUuTien= 1 then '#ff6b77' else 'rgb(3, 155, 229)' -- uutiencao: hongnhat, nguoclai: congviec xanhtroi
			end end as color,
			TenNhanVien, 
    		ISNULL(LoaiDoiTuong,0) as LoaiDoiTuong, 
    		ISNULL(MaDoiTuong,'') as MaDoiTuong, 
    		ISNULL(TenDoiTuong,'') as TenDoiTuong, 
    		ISNULL(DienThoai,'') as DienThoai, 
			ISNULL(TenNhomDoiTuongs,N'Nhóm mặc định') as TenNhomDoiTuongs,    		
			dt.ID_NguonKhach,
    		ISNULL(TenLoaiTuVanLichHen,'') as TenLoaiTuVanLichHen, ISNULL(nk.TenNguonKhach,'') as TenNguonKhach,
			case when dt.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' else ISNULL(dt.IDNhomDoiTuongs,'00000000-0000-0000-0000-000000000000') end as IDNhomDoiTuongs,
			case when NgayBatDau > @today then 0 else 1 end as DateSort
			
    	from
    		(select *, format(NgayGio, 'yyyy-MM-dd') as NgayBatDau
    		from
    			(-- lichhen
    			select ID,
    				Ma_TieuDe,
    				ID_DonVi, 
    				ID_KhachHang, 
    				ISNULL(ID_LoaiTuVan,'00000000-0000-0000-0000-000000000000') as ID_LoaiTuVan,
    				ID_NhanVien,
    				ISNULL(ID_NhanVienQuanLy,'00000000-0000-0000-0000-000000000000') as ID_NhanVienQuanLy,
    				NgayTao,
    				NgayHenGap as NgayGio,
    				NgayGioKetThuc,
    				NgayHoanThanh,
    				TrangThai,
    				GhiChu,
					NoiDung,
    				NguoiTao,
    				MucDoUuTien,
    				KetQua,
    				NhacNho,			
    				KieuNhacNho,
    				KieuLap,
    				SoLanLap, 
    				GiaTriLap, 
    				TuanLap, 
    				TrangThaiKetThuc ,
    				GiaTriKetThuc,
    				3 as PhanLoai,
    				'0' as CaNgay,
    				ExistDB, 
    				ID_Parent,
    				NgayCu
    			from @tblCalendar
    			where exists (select ID from @tblTrangThai tt where tt.TrangThaiCV = TrangThai) 
    			and NgayHenGap between @FromDate and @ToDate
    
    			union all
    			-- cong viec
    			select 
    					cs.ID,
    					Ma_TieuDe,
    					cs.ID_DonVi, 
    					ID_KhachHang, 
    					ISNULL(ID_LoaiTuVan,'00000000-0000-0000-0000-000000000000') as ID_LoaiTuVan,
    					ID_NhanVien,
    					ISNULL(ID_NhanVienQuanLy,'00000000-0000-0000-0000-000000000000') as ID_NhanVienQuanLy,
    					cs.NgayTao,
    					NgayGio,
    					NgayGioKetThuc,
    					NgayHoanThanh,
    					cs.TrangThai,
    					cs.GhiChu,
						cs.NoiDung,
    					cs.NguoiTao,
    					MucDoUuTien,
    					KetQua,
    					cs.NhacNho,
    					ISNULL(KieuNhacNho,0) as KieuNhacNho,
    					ISNULL(KieuLap,0) as KieuLap,
    					ISNULL(SoLanLap,0) as SoLanLap,
    					ISNULL(GiaTriLap,'') as GiaTriLap,
    					ISNULL(TuanLap,0) as TuanLap,
    					ISNULL(TrangThaiKetThuc,0) as TrangThaiKetThuc,
    					ISNULL(GiaTriKetThuc,'') as GiaTriKetThuc,
    					4 as PhanLoai,
    					ISNULL(cs.CaNgay,'0') as CaNgay,
    					'1' as ExistDB,
    					ID_Parent,
    					NgayCu
    				from ChamSocKhachHangs cs
    				where PhanLoai= 4
    				and exists (select ID from @tblTrangThai tt where tt.TrangThaiCV = TrangThai) 
    				and NgayGio between  @FromDate and  @ToDate
    				) a
    			where 
				exists (select ID from @tblNVPhuTrach nvpt where nvpt.ID= a.ID_NhanVien)
    			and 
				exists (select ID from @tblLoaiCV loaiCV where loaiCV.ID = a.ID_LoaiTuVan)
				and exists (select ID from @tblChiNhanh cn where cn.id = a.ID_DonVi)
    		)b
    		left join DM_DoiTuong dt on b.ID_KhachHang= dt.ID
    		left join NS_NhanVien nv on b.ID_NhanVien= nv.ID   	
			left join DM_LoaiTuVanLichHen loai on b.ID_LoaiTuVan= loai.ID
    		left join DM_NguonKhachHang nk on dt.ID_NguonKhach= nk.ID
    		where (dt.LoaiDoiTuong like @LoaiDoiTuong OR LoaiDoiTuong is null)
    		and
			b.PhanLoai like @PhanLoai
    		and b.MucDoUuTien like @DoUuTien
    		and ISNULL(dt.ID,'00000000-0000-0000-0000-000000000000') like @IDKhachHang
			AND
		((select count(Name) from @tblSearchString tblS where 
				b.Ma_TieuDe like '%'+tblS.Name+'%' 
    			or b.GhiChu like N'%'+tblS.Name+'%'
				or dt.TenDoiTuong like '%'+tblS.Name+'%'
    			or dt.TenDoiTuong_KhongDau  like '%'+tblS.Name+'%'
				or dt.MaDoiTuong like '%'+tblS.Name+'%'
    			or dt.DienThoai  like '%'+tblS.Name+'%'
				or nv.MaNhanVien like N'%'+tblS.Name+'%'
				or nv.TenNhanVien like N'%'+tblS.Name+'%'					
    			or nv.TenNhanVienKhongDau like '%'+tblS.Name+'%')=@count or @count=0)    
				
			),
			count_cte
			as
			(
			select count (ID) as TotalRow,
				CEILING(count(ID)/CAST(@PageSize as float )) as TotalPage
			from data_cte
			)
			select dt.*, cte.*
			from data_cte dt
			cross join count_cte cte
			order by dt.DateSort, dt.NgayGio
		
			
END");

			Sql(@"ALTER PROCEDURE [dbo].[getListNhanVien_allDonVi]
    @ID_ChiNhanh [nvarchar](max)
AS
BEGIN
    Select 
    	nv.ID,
    	nv.MaNhanVien,
		nv.TenNhanVien,
		max(ct.ID_PhongBan) as ID_PhongBan
    	From NS_NhanVien nv
    	inner join NS_QuaTrinhCongTac ct on nv.ID = ct.ID_NhanVien
    	where ct.ID_DonVi in (Select * from splitstring(@ID_ChiNhanh)) 
		and (nv.TrangThai is null or nv.TrangThai = 1) and nv.DaNghiViec = 0
    	GROUP by nv.ID, nv.MaNhanVien, nv.TenNhanVien
END");

			Sql(@"ALTER PROCEDURE [dbo].[getlistNhanVien_CaiDatChietKhau]
    @ID_DonVi [uniqueidentifier],
    @Text_NhanVien [nvarchar](max),
    @Text_NhanVien_TV [nvarchar](max),
    @TrangThai [nvarchar](max)
AS
BEGIN
	
    Select a.ID as ID_NhanVien,
    	a.MaNhanVien,
		a.TenNhanVien, 
		a.DienThoaiDiDong, 
		a.GioiTinh,
		a.MaPhongBan,
		a.ID_PhongBan,
		ISNULL(AnhNV.URLAnh, '') AS URLAnh,
		ISNULL(a.MaPhongBan, '') AS MaPhongBan,
		ISNULL(a.TenPhongBan, N'Phòng mặc định') AS TenPhongBan		
    	From
    	(   	
			Select DISTINCT nv.ID, 
				nv.MaNhanVien, 
				nv.TenNhanVien,
				nv.TenNhanVienChuCaiDau, 
				nv.TenNhanVienKhongDau, 
				nv.DienThoaiDiDong, 			
    			Case when nv.TrangThai is null then 1 else nv.TrangThai end as TrangThai,
    			Case when ck.ID_NhanVien is null then 0 else 1 end as CaiDat,
				nv.GioiTinh,
				ct.ID_PhongBan,
				pb.MaPhongBan,
				pb.TenPhongBan
    		From NS_NhanVien nv
    		join NS_QuaTrinhCongTac ct on nv.ID = ct.ID_NhanVien
			join NS_PhongBan pb on ct.ID_PhongBan= pb.ID
    		left join (select ck.ID_NhanVien
					from ChietKhauMacDinh_NhanVien ck 
					where ID_DonVi= @ID_DonVi 
					and exists (select ID from DonViQuiDoi qd where ID_DonViQuiDoi= qd.ID and Xoa='0')
					group by ck.ID_NhanVien) ck on nv.ID= ck.ID_NhanVien    
    		where (nv.MaNhanVien like @Text_NhanVien or nv.MaNhanVien like @Text_NhanVien_TV 
    		or nv.DienThoaiDiDong like @Text_NhanVien_TV or nv.TenNhanVienKhongDau like @Text_NhanVien or nv.TenNhanVienChuCaiDau like @Text_NhanVien)
    		and DaNghiViec != '1'
    		and ct.ID_DonVi = @ID_DonVi
    	)a
		left join (SELECT ID_NhanVien, URLAnh FROM NS_NhanVien_Anh WHERE SoThuTu = 1 GROUP BY ID_NhanVien, URLAnh) as AnhNV
		ON a.ID = AnhNV.ID_NhanVien
    	where a.CaiDat like @TrangThai
    	and a.TrangThai != 0
    
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListPhuTungTheoDoiByIdXe_v1]
    @IdXe [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert statements for procedure here
	--DECLARE @IdXe UNIQUEIDENTIFIER;
	--SET @IdXe = '5eeaceb6-a3b1-4496-81ec-fcd39640ee6b';
    	-- Lấy danh sách phụ tùng đang theo dõi
    	DECLARE @tblPhuTungTheoDoi TABLE(Id UNIQUEIDENTIFIER, IdHoaDon UNIQUEIDENTIFIER, IdDonViQuiDoi UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, LoaiHoaDon INT, IdXe UNIQUEIDENTIFIER, 
    	IdHangHoa UNIQUEIDENTIFIER, MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), DinhMucBaoDuong FLOAT, ThoiGianHoatDong FLOAT, ThoiGianConLai FLOAT);
    	INSERT INTO @tblPhuTungTheoDoi
    	select ct.ID, hd.ID, dvqd.ID, hd.NgayLapHoaDon, hd.LoaiHoaDon, hd.ID_Xe, hh.ID AS IdHangHoa, dvqd.MaHangHoa, hh.TenHangHoa, 0, 0, 0 from BH_HoaDon hd
    	INNER JOIN BH_HoaDon_ChiTiet ct ON hd.ID = ct.ID_HoaDon
    	INNER JOIN DonViQuiDoi dvqd ON dvqd.ID = ct.ID_DonViQuiDoi
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa AND dvqd.LaDonViChuan = 1
    	WHERE hd.ID_Xe = @IdXe AND hd.LoaiHoaDon = 29;

    	-- Lấy thời gian của hóa đơn sửa chữa gần nhất
    	DECLARE @tblHoaDonSuaChua TABLE(Id UNIQUEIDENTIFIER, IdHoaDon UNIQUEIDENTIFIER, IdXe UNIQUEIDENTIFIER, IdHangHoa UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME,
    	MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX));
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT MAX(ct.ID) AS Id, hd.ID, pttd.IdXe, pttd.IdHangHoa, MAX(hd.NgayLapHoaDon) AS NgayLapHoaDon, dvqd.MaHangHoa, hh.TenHangHoa FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN Gara_PhieuTiepNhan ptn ON ptn.ID_Xe = pttd.IdXe
    	INNER JOIN BH_HoaDon hd	ON hd.ID_PhieuTiepNhan = ptn.ID
    	INNER JOIN BH_HoaDon_ChiTiet ct ON hd.ID = ct.ID_HoaDon
    	INNER JOIN DonViQuiDoi dvqd ON dvqd.ID = ct.ID_DonViQuiDoi AND dvqd.ID_HangHoa = pttd.IdHangHoa
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
    	WHERE hh.LoaiHangHoa = 1
    	GROUP BY pttd.IdXe, pttd.IdHangHoa, dvqd.MaHangHoa, hh.TenHangHoa, hd.ID;

    	-- Cập nhật thời gian bắt đầu tính thời gian hoạt động dựa theo thời gian hóa đơn sửa chữa gần nhất
    	UPDATE pttd
    	SET pttd.NgayLapHoaDon = hdsc.NgayLapHoaDon, pttd.LoaiHoaDon = 25
    	FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN (SELECT IdXe, IdHangHoa, MAX(NgayLapHoaDon) AS NgayLapHoaDon FROM @tblHoaDonSuaChua GROUP BY IdXe, IdHangHoa) hdsc 
		ON pttd.IdXe = hdsc.IdXe AND pttd.IdHangHoa = hdsc.IdHangHoa;
    
    	INSERT INTO @tblPhuTungTheoDoi
    	SELECT hdsc.Id, hdsc.IdHoaDon, dvqd.ID, hdsc.NgayLapHoaDon, 25, hdsc.IdXe, hdsc.IdHangHoa, hdsc.MaHangHoa, hdsc.TenHangHoa, 0, 0, 0 FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN @tblPhuTungTheoDoi pttd ON hdsc.IdXe = pttd.IdXe AND hdsc.IdHangHoa = pttd.IdHangHoa
    	INNER JOIN DonViQuiDoi dvqd ON dvqd.ID_HangHoa = hdsc.IdHangHoa AND dvqd.LaDonViChuan = 1
    	WHERE pttd.Id IS NULL;
    
    
    	-- Lấy định mức bảo dưỡng trong danh mục hàng hóa
    	DECLARE @tblDinhMucBaoDuong TABLE (Id UNIQUEIDENTIFIER, DinhMucBaoDuong INT);
    	INSERT INTO @tblDinhMucBaoDuong
    	SELECT pttd.Id, 
    	IIF(bdct.LoaiGiaTri = 1, bdct.GiaTri * 24, 
    	IIF(bdct.LoaiGiaTri = 2, bdct.GiaTri * 24 * 30, 
    	IIF(bdct.LoaiGiaTri = 3, bdct.GiaTri * 24 * 365, 
		IIF(bdct.LoaiGiaTri = 5, bdct.GiaTri, 0)))) AS SoGioDinhMuc FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN DM_HangHoa_BaoDuongChiTiet bdct ON pttd.IdHangHoa = bdct.ID_HangHoa AND bdct.BaoDuongLapDinhKy = 1;
    
    	-- Cập nhật định mức bảo dưỡng
    	UPDATE pttd
    	SET pttd.DinhMucBaoDuong = dmbd.DinhMucBaoDuong
    	FROM @tblPhuTungTheoDoi pttd
    	INNER JOIN @tblDinhMucBaoDuong dmbd ON pttd.Id = dmbd.Id;
    
    	-- Cập nhật thời gian hoạt động và thời gian còn lại
    	UPDATE @tblPhuTungTheoDoi
    	SET ThoiGianHoatDong = ISNULL(dbo.GetSumSoGioHoatDongByIdXe(IdXe, NgayLapHoaDon), 0);
    	UPDATE @tblPhuTungTheoDoi
    	SET ThoiGianConLai = IIF(DinhMucBaoDuong = 0, 0, DinhMucBaoDuong - ThoiGianHoatDong);
    
    	SELECT * FROM @tblPhuTungTheoDoi ORDER BY ThoiGianConLai;
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetMaDoiTuong_Max]
    @LoaiDoiTuong [int]
AS
BEGIN
    DECLARE @MaDTuongOffline varchar(5);
    DECLARE @MaDTuongTemp varchar(5);
    DECLARE @Return float
    
    if @LoaiDoiTuong = 1 
    	BEGIN
    		SET @MaDTuongOffline ='KHO'
    		SET @MaDTuongTemp='KH'
    	END 
    else if @LoaiDoiTuong = 2 
    	BEGIN
    		SET @MaDTuongOffline ='NCCO'
    		SET @MaDTuongTemp='NCC'
    	END 
    else if @LoaiDoiTuong = 3
    BEGIN
    	SET @MaDTuongOffline ='BHO';
    	SET @MaDTuongTemp='BH';
    END
    
    	-- get list DoiTuong not offline
    	SELECT @Return = MAX(CAST (dbo.udf_GetNumeric(MaDoiTuong) AS float))
    	FROM DM_DoiTuong 
		WHERE LoaiDoiTuong = @LoaiDoiTuong 
		and MaDoiTuong like @MaDTuongTemp +'%'  AND MaDoiTuong not like @MaDTuongOffline + '%'		
    
    	if	@Return is null 
    		select Cast(0 as float) as MaxCode
    	else 
    		select @Return as MaxCode
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetMaHoaDonMax_byTemp]
	@LoaiHoaDon int,
	@ID_DonVi uniqueidentifier,
	@NgayLapHoaDon datetime
AS
BEGIN	
	SET NOCOUNT ON;

	declare @tblLoaiHD table (LoaiHD int)
	if @LoaiHoaDon = 4 or @LoaiHoaDon = 13 or @LoaiHoaDon = 14 ---- nhapkho noibo + nhaphangthua = dùng chung mã với nhập kho nhà cung cấp
		begin
			set @LoaiHoaDon = 4
			insert into @tblLoaiHD values (4),(13),(14)
		end
	else 
		insert into @tblLoaiHD values (@LoaiHoaDon)

	DECLARE @Return float = 1
	declare @lenMaMax int = 0
	DECLARE @isDefault bit = (select top 1 SuDungMaChungTu from HT_CauHinhPhanMem where ID_DonVi= @ID_DonVi)-- co/khong thiet lap su dung Ma MacDinh
	DECLARE @isSetup int = (select top 1 ID_LoaiChungTu from HT_MaChungTu where ID_LoaiChungTu = @LoaiHoaDon)-- da ton tai trong bang thiet lap chua

	if @isDefault='1' and @isSetup is not null
		begin
			DECLARE @machinhanh varchar(15) = (select MaDonVi from DM_DonVi where ID= @ID_DonVi)
			DECLARE @lenMaCN int = Len(@machinhanh)

			DECLARE @isUseMaChiNhanh varchar(15), @kituphancach1 varchar(1),  @kituphancach2 varchar(1),  @kituphancach3 varchar(1),
			 @dinhdangngay varchar(8), @dodaiSTT INT, @kihieuchungtu varchar(10)

			 select @isUseMaChiNhanh = SuDungMaDonVi, 
				@kituphancach1= KiTuNganCach1,
				@kituphancach2 = KiTuNganCach2,
				@kituphancach3= KiTuNganCach3,
				@dinhdangngay = NgayThangNam,
				@dodaiSTT = CAST(DoDaiSTT AS INT),
				@kihieuchungtu = MaLoaiChungTu
			 from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon 

		
			DECLARE @lenMaKiHieu int = Len(@kihieuchungtu);
			DECLARE @namthangngay varchar(10) = convert(varchar(10), @NgayLapHoaDon, 112)
			DECLARE @year varchar(4) = Left(@namthangngay,4)
			DECLARE @date varchar(4) = right(@namthangngay,2)
			DECLARE @month varchar(4) = substring(@namthangngay,5,2)
			DECLARE @datecompare varchar(10)='';
			
			if	@isUseMaChiNhanh='0'
				begin 
					set @machinhanh=''
					set @lenMaCN=0
				end

			if @dinhdangngay='ddMMyyyy'
				set @datecompare = CONCAT(@date,@month,@year)
			else	
				if @dinhdangngay='ddMMyy'
					set @datecompare = CONCAT(@date,@month,right(@year,2))
				else 
					if @dinhdangngay='MMyyyy'
						set @datecompare = CONCAT(@month,@year)
					else	
						if @dinhdangngay='MMyy'
							set @datecompare = CONCAT(@month,right(@year,2))
						else
							if @dinhdangngay='yyyyMMdd'
								set @datecompare = CONCAT(@year,@month,@date)
							else 
								if @dinhdangngay='yyMMdd'
									set @datecompare = CONCAT(right(@year,2),@month,@date)
								else	
									if @dinhdangngay='yyyyMM'
										set @datecompare = CONCAT(@year,@month)
									else	
										if @dinhdangngay='yyMM'
											set @datecompare = CONCAT(right(@year,2),@month)
										else 
											if @dinhdangngay='yyyy'
												set @datecompare = @year							

			DECLARE @sMaFull varchar(50) = concat(@machinhanh,@kituphancach1,@kihieuchungtu,@kituphancach2, @datecompare, @kituphancach3)	

			declare @sCompare varchar(30) = @sMaFull
			if @sMaFull= concat(@kihieuchungtu,'_') set @sCompare = concat(@kihieuchungtu,'[_]') -- like %_% không nhận kí tự _ nên phải [_] theo quy tắc của sql
			DECLARE @MaHoaDonMax1 NVARCHAR(MAX);
			select TOP 1 @MaHoaDonMax1 = Mahoadon 
				from BH_HoaDon 
				where MaHoaDon like @sCompare +'%'  AND LEN(MaHoaDon) = LEN(@sMaFull) + @dodaiSTT
				and LoaiHoaDon in (select LoaiHD from @tblLoaiHD)
				ORDER BY MaHoaDon desc;

			set @Return = CAST(dbo.udf_GetNumeric(RIGHT(@MaHoaDonMax1,@dodaiSTT))AS float);
			

			-- lay chuoi 000
			declare @stt int =0;
			declare @strstt varchar (10) ='0'
			while @stt < @dodaiSTT- 1
				begin
					set @strstt= CONCAT('0',@strstt)
					SET @stt = @stt +1;
				end 
			declare @lenSst int = len(@strstt)
			if	@Return is null 
					select CONCAT(@sMaFull,left(@strstt,@lenSst-1),1) as MaxCode-- left(@strstt,@lenSst-1): bỏ bớt 1 số 0			
			else 
				begin
					set @Return =  @Return + 1
					set @lenMaMax =  len(@Return)

					-- neu @Return là 1 số quá lớn --> mã bị chuyển về dạng e+10
					declare @madai nvarchar(max)= CONCAT(@sMaFull, CONVERT(numeric(22,0), @Return))
					select 
						case @lenMaMax							
							when 1 then CONCAT(@sMaFull,left(@strstt,@lenSst-1),@Return)
							when 2 then case when @lenSst - 2 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-2), @Return) else @madai end
							when 3 then case when @lenSst - 3 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-3), @Return) else @madai end
							when 4 then case when @lenSst - 4 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-4), @Return) else @madai end
							when 5 then case when @lenSst - 5 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-5), @Return) else @madai end
							when 6 then case when @lenSst - 6 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-6), @Return) else @madai end
							when 7 then case when @lenSst - 7 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-7), @Return) else @madai end
							when 8 then case when @lenSst - 8 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-8), @Return) else @madai end
							when 9 then case when @lenSst - 9 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-9), @Return) else @madai end
							when 10 then case when @lenSst - 10 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-10), @Return) else @madai end
						else 
							case when  @lenMaMax > 10
								 then iif(@lenSst - 10 > -1, CONCAT(@sMaFull, left(@strstt,@lenSst-10), @Return),  @madai)
								 else '' end
						end as MaxCode					
				end 
		end
	else
		begin
			declare @machungtu varchar(10) = (select top 1 MaLoaiChungTu from DM_LoaiChungTu where ID= @LoaiHoaDon)
			declare @lenMaChungTu int= LEN(@machungtu)
			DECLARE @MaHoaDonMax NVARCHAR(MAX);
			IF @LoaiHoaDon = 30
			BEGIN
				select TOP 1 @MaHoaDonMax = MaPhieu
				from Gara_Xe_PhieuBanGiao 
				where SUBSTRING(MaPhieu, 1, len(@machungtu)) = @machungtu 
				and CHARINDEX('O',MaPhieu) = 0 
				AND LEN(MaPhieu) = 10 + @lenMaChungTu 
				AND ISNUMERIC(RIGHT(MaPhieu,LEN(MaPhieu)- @lenMaChungTu)) = 1
				ORDER BY MaPhieu DESC;
			END
			ELSE
			BEGIN
				declare @maOffline nvarchar(max) =''
				if @LoaiHoaDon= 1 or @LoaiHoaDon= 25 set @maOffline='HDO'
				if @LoaiHoaDon= 3 set @maOffline='DHO'
				if @LoaiHoaDon= 19 set @maOffline='GDVO'

				select TOP 1 @MaHoaDonMax = MaHoaDon
				from BH_HoaDon where SUBSTRING(MaHoaDon, 1, len(@machungtu)) = @machungtu and CHARINDEX(@maOffline,MaHoaDon) = 0 				
				and LoaiHoaDon in (select LoaiHD from @tblLoaiHD)
				AND LEN(MaHoaDon) = 10 + @lenMaChungTu AND ISNUMERIC(RIGHT(MaHoaDon,LEN(MaHoaDon)- @lenMaChungTu)) = 1
				ORDER BY MaHoaDon DESC;
			END
			SET @Return = CAST(dbo.udf_GetNumeric(RIGHT(@MaHoaDonMax,LEN(@MaHoaDonMax)- @lenMaChungTu))AS float);
			
			-- do dai STT (toida = 10)
			if	@Return is null 
					select
						case when @lenMaChungTu = 2 then CONCAT(@machungtu, '00000000',1)
							when @lenMaChungTu = 3 then CONCAT(@machungtu, '0000000',1)
							when @lenMaChungTu = 4 then CONCAT(@machungtu, '000000',1)
							when @lenMaChungTu = 5 then CONCAT(@machungtu, '00000',1)
						else CONCAT(@machungtu,'000000',1)
						end as MaxCode
			else 
				begin
					set @Return = @Return + 1
					set @lenMaMax = len(@Return)
					select 
						case @lenMaMax
							when 1 then CONCAT(@machungtu,'000000000',@Return)
							when 2 then CONCAT(@machungtu,'00000000',@Return)
							when 3 then CONCAT(@machungtu,'0000000',@Return)
							when 4 then CONCAT(@machungtu,'000000',@Return)
							when 5 then CONCAT(@machungtu,'00000',@Return)
							when 6 then CONCAT(@machungtu,'0000',@Return)
							when 7 then CONCAT(@machungtu,'000',@Return)
							when 8 then CONCAT(@machungtu,'00',@Return)
							when 9 then CONCAT(@machungtu,'0',@Return)								
						else CONCAT(@machungtu,CAST(@Return  as decimal(22,0))) end as MaxCode						
				end 
		end
		
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetQuyChiTiet_byIDQuy]
    @ID [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
	declare @ngaylapPhieuThu datetime = (select top 1 NgayLapHoaDon from Quy_HoaDon where ID= @ID)

	---- get allhoadon lienquan by idSoQuy
	select ID_HoaDonLienQuan  into #tblHoaDon
	from Quy_HoaDon_ChiTiet qct
	where qct.ID_HoaDon = @ID	

	---- get phieuthu/chi lienquan hoadon
		select 
			qct.ID_HoaDonLienQuan,
			qct.ID_DoiTuong,
			sum(qct.TienThu) as DaThuTruoc
	into #tblThuTruoc
	 from Quy_HoaDon qhd
    join Quy_HoaDon_ChiTiet qct on qhd.ID = qct.ID_HoaDon
	where exists
		(select qct2.ID_HoaDonLienQuan from #tblHoaDon qct2 
		where qct.ID_HoaDonLienQuan = qct2.ID_HoaDonLienQuan)
	and qhd.ID != @ID
	and qhd.TrangThai ='1'
	group by qct.ID_HoaDonLienQuan,qct.ID_DoiTuong

	---- if hd xuly from dathang --> get infor hd dathang
	select hdd.ID, hdMua.ID as ID_HoaDonMua, hdMua.NgayLapHoaDon into #tblDat
	from BH_HoaDon hdd
	join
	(
		select hd.ID, hd.ID_HoaDon, hd.NgayLapHoaDon
		from #tblHoaDon tmp
		join BH_HoaDon hd on tmp.ID_HoaDonLienQuan= hd.ID
	) hdMua on hdd.ID = hdMua.ID_HoaDon
	where hdd.LoaiHoaDon = 3 and hdd.ChoThanhToan='0'

	---- get phieuthu from dathang
		select thuDH.ID_HoaDonMua, 
				thuDH.ID_DoiTuong,
				thuDH.ThuDatHang
		into #tblThuDH
			from
			(
				select tblDH.ID_HoaDonMua,
					tblDH.ID_DoiTuong,
					sum(tblDH.TienThu) as ThuDatHang,		
					ROW_NUMBER() OVER(PARTITION BY tblDH.ID ORDER BY tblDH.NgayLapHoaDon ASC) AS isFirst	--- group by hdDat, sort by ngaylap hdxuly
				from
				(			
						select hdd.ID_HoaDonMua, hdd.NgayLapHoaDon,		
							hdd.ID,
							qct.ID_DoiTuong,
							iif(qhd.LoaiHoaDon = 11, qct.TienThu, -qct.TienThu) as TienThu			
						from Quy_HoaDon_ChiTiet qct
						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
						join #tblDat hdd on hdd.ID= qct.ID_HoaDonLienQuan				
						where (qhd.TrangThai= 1 Or qhd.TrangThai is null)
				) tblDH group by tblDH.ID_HoaDonMua, tblDH.ID,tblDH.NgayLapHoaDon, tblDH.ID_DoiTuong
		) thuDH where thuDH.isFirst= 1 

	---- get chiphi dichvu NCC
	select 
			cp.ID_HoaDon,
			sum(cp.ThanhTien) as PhaiThanhToan
		into #tblChiPhi
		from BH_HoaDon_ChiPhi cp
		left join #tblHoaDon hd on cp.ID_HoaDon = hd.ID_HoaDonLienQuan
		group by cp.ID_HoaDon

    select qhd.id, qct.ID_HoaDon, qhd.MaHoaDon, qhd.NguoiTao, qhd.LoaiHoaDon, qhd.TongTienThu, qhd.ID_NhanVien, qhd.NoiDungThu,
		 qhd.ID_DonVi,	qhd.HachToanKinhDoanh, qhd.PhieuDieuChinhCongNo,qhd.NguoiSua, isnull(qhd.TrangThai, '1') as TrangThai,
    	  iif(qct.HinhThucThanhToan=1, qct.TienThu,0) as TienMat, 
		  iif(qct.HinhThucThanhToan=2 or qct.HinhThucThanhToan=3 , qct.TienThu,0) as TienGui, 
			qct.TienThu, qct.DiemThanhToan, qct.ID_TaiKhoanNganHang, qct.ID_KhoanThuChi, 
		   qhd.NgayLapHoaDon as NgayLapPhieuThu,
    	   qct.ID_DoiTuong,
		   qct.ID_BangLuongChiTiet,
    	   qct.ID_HoaDonLienQuan,
    	   qct.ID_NhanVien as ID_NhanVienCT, -- thu/chi cho NV nao
    	   qct.HinhThucThanhToan,
    	   cast(iif(qhd.LoaiHoaDon = 11,'1','0') as bit) as LaKhoanThu,
    	   iif(qct.LoaiThanhToan = 1,1,0) as LaTienCoc,
    	   isnull(hd.MaHoaDon,N'Thu thêm') as MaHoaDonHD,    	  
		   nv.TenNhanVien,
		   dt.MaDoiTuong as MaDoiTuong, 
		   dt.TenDoiTuong as NguoiNopTien, 	
		   dt.DienThoai as SoDienThoai,
    	   case dt.LoaiDoiTuong
    		when 1 then 1
    		when 2 then 2
    		when 3 then 3
    		else 0 end as LoaiDoiTuong,
    		iif(qhd.TrangThai ='0', N'Đã hủy', N'Đã thanh toán') as GhiChu,	  
    	   iif( hd.NgayLapHoaDon is null, qhd.NgayLapHoaDon, hd.NgayLapHoaDon) as NgayLapHoaDon,
    	   case qct.HinhThucThanhToan
    			when 1 then  N'Tiền mặt'
    			when 2 then  N'POS'
    			when 3 then  N'Chuyển khoản'
    			when 4 then  N'Thu từ thẻ'
    			when 5 then  N'Đổi điểm'
    			when 6 then  N'Thu từ cọc'
    		end as PhuongThuc,
			ktc.NoiDungThuChi,
			iif(ktc.LaKhoanThu is null,  IIF(qhd.LoaiHoaDon=11,'1','0'), ktc.LaKhoanThu) as LaKhoanThu,
			iif(tk.TaiKhoanPOS ='1',tk.TenChuThe,'') as TenTaiKhoanPOS,
			iif(tk.TaiKhoanPos ='0',tk.TenChuThe,'') as TenTaiKhoanNOTPOS,	
			isnull(hd.LoaiHoaDon,0) as LoaiHoaDonHD,
			isnull(iif(dt.LoaiDoiTuong =3, hd.TongTienThueBaoHiem,iif(hd.TongThueKhachHang >0, hd.TongThueKhachHang, hd.TongTienThue)),0) as TongTienThue,
			isnull(case dt.LoaiDoiTuong
				when 2 then iif(hd.LoaiHoaDon in (4,7), hd.PhaiThanhToan,cp.PhaiThanhToan)
				when 1 then hd.PhaiThanhToan
				when 3 then hd.PhaiThanhToanBaoHiem
			end,0) as TongThanhToanHD,		
			isnull(thu.DaThuTruoc,0) as DaThuTruoc,
			tk.TaiKhoanPOS,
			nh.TenNganHang,
			nh.ChiPhiThanhToan,
			nh.MacDinh,
			nh.TheoPhanTram,
			nh.ThuPhiThanhToan
    from Quy_HoaDon qhd
    join Quy_HoaDon_ChiTiet qct on qhd.ID = qct.ID_HoaDon
    left join BH_HoaDon hd on qct.ID_HoaDonLienQuan= hd.ID
	left join #tblChiPhi cp on hd.ID= cp.ID_HoaDon and qct.ID_HoaDonLienQuan = cp.ID_HoaDon
    left join DM_DoiTuong dt on qct.ID_DoiTuong= dt.ID
	left join NS_NhanVien nv on qhd.ID_NhanVien= nv.ID
	left join Quy_KhoanThuChi ktc on qct.ID_KhoanThuChi = ktc.ID
	left join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang = tk.ID
	left join DM_NganHang nh on tk.ID_NganHang = nh.ID
	left join (
		select 
			thutruoc.ID_HoaDonLienQuan,
			thutruoc.ID_DoiTuong,
			sum(isnull(DaThuTruoc,0)) as DaThuTruoc
		from
		(
		select tmp.ID_HoaDonLienQuan,tmp.ID_DoiTuong, isnull(tmp.DaThuTruoc,0) as DaThuTruoc
		from #tblThuTruoc tmp 
		union all
		select thuDH.ID_HoaDonMua, thuDH.ID_DoiTuong, isnull(thuDH.ThuDatHang,0) as DaThuTruoc
		from #tblThuDH thuDH 
		) thutruoc group by thutruoc.ID_HoaDonLienQuan, thutruoc.ID_DoiTuong
	) thu on thu.ID_HoaDonLienQuan = qct.ID_HoaDonLienQuan and thu.ID_DoiTuong = qct.ID_DoiTuong
    where qhd.ID= @ID
	order by hd.NgayLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetSoDuTheGiaTri_ofKhachHang]
    @ID_DoiTuong [uniqueidentifier],
    @DateTime [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    	set @DateTime= DATEADD(DAY,1,@DateTime)
    	select 
    		TongThuTheGiaTri,
			TraLaiSoDu,
			SuDungThe, 
			HoanTraTheGiatri,
    		ThucThu,
			PhaiThanhToan,
			SoDuTheGiaTri,
    		iif(CongNoThe<0,0,CongNoThe) as CongNoThe
    	from
    	(
    	select 		
    		sum(TongThuTheGiaTri) - sum(TraLaiSoDu) as TongThuTheGiaTri, 
			sum(TraLaiSoDu) as TraLaiSoDu,
    		cast(sum(SuDungThe) as float) as SuDungThe,
    		cast(sum(HoanTraTheGiatri) as float) as HoanTraTheGiatri,
    		cast(sum(ThucThu) as float) as ThucThu,
    		cast(sum(PhaiThanhToan) as float) as PhaiThanhToan,
    		cast(SUM(TongThuTheGiaTri)- sum(TraLaiSoDu)  - SUM(SuDungThe) + SUM(HoanTraTheGiatri) as float) as SoDuTheGiaTri,
    		cast(sum(PhaiThanhToan) - sum(TraLaiSoDu) - sum(ThucThu) as float) as CongNoThe
    	from (
    		-- so du nap the va thuc te phai thanh toan
    		SELECT 
    			TongTienHang as TongThuTheGiaTri,
    			0 as SuDungThe,
    			0 as HoanTraTheGiatri,
    			0 as ThucThu,
    			hd.PhaiThanhToan, -- dieu chinh the (khong lien quan den cong no)
				0 as TraLaiSoDu
    		FROM BH_HoaDon hd
    		where hd.ID_DoiTuong like @ID_DoiTuong and hd.ChoThanhToan ='0' and hd.LoaiHoaDon in (22,23) 
    		and hd.NgayLapHoaDon  < @DateTime
    
    		union all
    		-- su dung the
    		SELECT 
    			0 as TongThuTheGiaTri,
    			SUM(qct.TienThu) as SuDungThe,
    			0 as HoanTraTheGiatri,
    			0 as ThucThu,
    			0 as PhaiThanhToan,
				0 as TraLaiSoDu
    		FROM Quy_HoaDon_ChiTiet qct
    		INNER JOIN Quy_HoaDon qhd
    		ON qct.ID_HoaDon = qhd.ID
    		WHERE qct.ID_DoiTuong like @ID_DoiTuong AND qhd.NgayLapHoaDon  < @DateTime and qhd.LoaiHoaDon = 11 
    		and (qhd.TrangThai = 1 or qhd.TrangThai is null)
    		and qct.HinhThucThanhToan=4

			
    		union all
    		---- hoàn trả số dư còn trong TGT cho khách --> giảm số dư
    		SELECT
    			0 as TongThuTheGiaTri,
    			0 as SuDungThe,
    			0 as HoanTraTheGiatri,
    			0 as ThucThu,
    			0 as PhaiThanhToan,
				SUM(hd.TongTienHang) as TraLaiSoDu
    		FROM BH_HoaDon hd
    		where hd.LoaiHoaDon= 32 and hd.ChoThanhToan= 0
    	
    		union all
    		-- hoàn trả tiền vào TGT ---> tăng số dư
    		SELECT
    			0 as TongThuTheGiaTri,
    			0 as SuDungThe,
    			SUM(qct.TienThu) as HoanTraTheGiatri,
    			0 as ThucThu,
    			0 as PhaiThanhToan,
				0 as TraLaiSoDu
    		FROM Quy_HoaDon_ChiTiet qct
    		INNER JOIN Quy_HoaDon qhd
    		ON qct.ID_HoaDon = qhd.ID
    		WHERE qct.ID_DoiTuong like @ID_DoiTuong AND qhd.NgayLapHoaDon  < @DateTime and qhd.LoaiHoaDon = 12
    		and (qhd.TrangThai = 1 or qhd.TrangThai is null)
    			and qct.HinhThucThanhToan=4
    
    		union all
    		-- thuc thu thegiatri
    		SELECT
    			0 as TongThuTheGiaTri,
    			0 as SuDungThe,
    			0 as HoanTraTheGiatri,
    			qct.TienThu as ThucThu,
    			0 as PhaiThanhToan,
				0 as TraLaiSoDu
    		from Quy_HoaDon_ChiTiet qct
    		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    		join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
    		where hd.ChoThanhToan ='0' and hd.LoaiHoaDon = 22 and qhd.NgayLapHoaDon < @DateTime and qct.ID_DoiTuong like @ID_DoiTuong
    		and (qhd.PhieuDieuChinhCongNo= 0 or PhieuDieuChinhCongNo  is  null)
    
    		-- thucthu do dieuchinh congno khachhang
    		union all
    		select
    			0 as TongThuTheGiaTri,
    			0 as SuDungThe,
    			0 as HoanTraTheGiatri,
    			qct.TienThu as ThucThu,
    			0 as PhaiThanhToan,
				0 as TraLaiSoDu
    		from Quy_HoaDon_ChiTiet qct
    		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    		where qhd.PhieuDieuChinhCongNo= 1 and qhd.LoaiHoaDon= 11
    		and (qhd.TrangThai= 1 or qhd.TrangThai is null)
    		and qct.ID_DoiTuong like @ID_DoiTuong
    		) tbl  
    		) tbl2
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetTonKho_byIDQuyDois]
    @ID_ChiNhanh [uniqueidentifier],
    @ToDate [datetime],
    @IDDonViQuyDois [nvarchar](max),
    @IDLoHangs [nvarchar](max)
AS
BEGIN
	 SET NOCOUNT ON;
    	declare @tblIDQuiDoi table (ID_DonViQuyDoi uniqueidentifier)
    	declare @tblIDLoHang table (ID_LoHang uniqueidentifier)
    
    	insert into @tblIDQuiDoi
    	select Name from dbo.splitstring(@IDDonViQuyDois) 
    	insert into @tblIDLoHang
    	select Name from dbo.splitstring(@IDLoHangs) where Name not like '%null%' and Name !=''

		
		---- get tonluyke theo thoigian 
		SELECT 
    		ID_DonViQuiDoi,
    		ID_HangHoa, 		
    		ID_LoHang,		
    		IIF(LoaiHoaDon = 10 AND ID_CheckIn = ID_DonViInput, TonLuyKe_NhanChuyenHang, TonLuyKe) AS TonKho, 
    		IIF(LoaiHoaDon = 10 AND ID_CheckIn = ID_DonViInput, GiaVon_NhanChuyenHang, GiaVon) AS GiaVon
		into #tblTon
    	FROM (
    		SELECT tbltemp.*, ROW_NUMBER() OVER (PARTITION BY tbltemp.ID_HangHoa, tbltemp.ID_LoHang, 
			tbltemp.ID_DonViInput ORDER BY tbltemp.ThoiGian DESC) AS RN 
		
    	FROM (
				select 
						hd.LoaiHoaDon, 
						hd.ID_DonVi,
						dvqd.ID_HangHoa,
						ct.ID_DonViQuiDoi,
						hd.ID_CheckIn, 					
						@ID_ChiNhanh as ID_DonViInput, 
    					IIF(hd.LoaiHoaDon = 10 AND hd.YeuCau = '4' AND hd.ID_CheckIn = @ID_ChiNhanh, 
						ct.TonLuyKe_NhanChuyenHang, ct.TonLuyKe) AS TonLuyKe,
    					ct.TonLuyKe_NhanChuyenHang,
    					IIF(hd.LoaiHoaDon = 10 AND hd.YeuCau = '4' AND hd.ID_CheckIn = @ID_ChiNhanh, 
    					ct.GiaVon_NhanChuyenHang, 
    					ct.GiaVon)/ISNULL(dvqd.TyLeChuyenDoi,1) AS GiaVon,
    					ct.GiaVon_NhanChuyenHang, 
    					ct.ID_LoHang ,
    					IIF(hd.LoaiHoaDon = 10 AND hd.YeuCau = '4' AND hd.ID_CheckIn = @ID_ChiNhanh,
						hd.NgaySua, hd.NgayLapHoaDon) AS ThoiGian
				from @tblIDQuiDoi qd
				join BH_HoaDon_ChiTiet ct on qd.ID_DonViQuyDoi = ct.ID_DonViQuiDoi
				join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
				join DonViQuiDoi dvqd on qd.ID_DonViQuyDoi= dvqd.ID
				join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
			where (hd.ID_DonVi= @ID_ChiNhanh or (hd.ID_CheckIn = @ID_ChiNhanh and hd.YeuCau = '4'))
			and hd.ChoThanhToan = 0 AND hd.LoaiHoaDon IN (1, 5, 7, 8, 4, 6, 9, 10,18)
			and (exists( select ID_LoHang from @tblIDLoHang lo2 where lo2.ID_LoHang= ct.ID_LoHang) Or hh.QuanLyTheoLoHang= 0)    
			) as tbltemp
    	WHERE tbltemp.ThoiGian < @ToDate) tblTonKhoTemp
    	WHERE tblTonKhoTemp.RN = 1;
		    
		select TenHangHoa, 
			lo.MaLoHang,
			qd2.ID_HangHoa,
			qd.ID_DonViQuyDoi as ID_DonViQuiDoi,
			qd2.GiaNhap,
			lo.ID as ID_LoHang, 
			isnull(tk.TonKho,0) as TonKho,
			isnull(tk.GiaVon,0) as GiaVon
		from @tblIDQuiDoi qd 	
		join DonViQuiDoi qd2 on qd.ID_DonViQuyDoi= qd2.ID 
		join DM_HangHoa hh on hh.ID = qd2.ID_HangHoa
		left join DM_LoHang lo on hh.ID = lo.ID_HangHoa and hh.QuanLyTheoLoHang = 1   
		left join #tblTon tk on qd.ID_DonViQuyDoi = tk.ID_DonViQuiDoi and qd2.ID = tk.ID_DonViQuiDoi
		and (tk.ID_LoHang = lo.ID or hh.QuanLyTheoLoHang =0)
		where (exists( select ID_LoHang from @tblIDLoHang lo2 where lo2.ID_LoHang= lo.ID) Or hh.QuanLyTheoLoHang= 0)
		order by qd.ID_DonViQuyDoi, lo.ID
END");

			Sql(@"ALTER PROCEDURE [dbo].[JqAuto_SearchXe]
    @TextSearch nvarchar(max) =null,	
	@StatusTN nvarchar(max) = null, ---- 1.chi get xe chua co trong xuong(khi them moi phieu tiep nhan), null. search all
	@IDCustomer nvarchar(max) = null, -- get list xe by chuxe
	@LaHangHoa int = null,
	@NguoiSoHuu int = null--null - all, 0- xe của kh, 1- xe của gara
AS
BEGIN
    SET NOCOUNT ON;

	declare @sql nvarchar(max) ='', @where nvarchar(max) ='', @paramDefined nvarchar(max) =''

	set @where =' where 1= 1 and xe.TrangThai = 1'
	
	if isnull(@TextSearch,'')!=''
		begin
			set @where= CONCAT(@where, ' and BienSo like N''%'' +  @TextSearch_In + ''%''')
		end

	if isnull(@StatusTN,'')!=''
		begin
			set @where= CONCAT(@where, ' and not exists (select ID_Xe from Gara_PhieuTiepNhan tn where xe.ID = tn.ID_Xe 
						and tn.TrangThai in (1,2 )) and not exists (SELECT IdXe FROM Gara_Xe_PhieuBanGiao pbg WHERE pbg.IdXe = xe.ID 
						AND pbg.TrangThai = 1 )')
		end

	if isnull(@IDCustomer,'')!=''
		begin
			set @where= CONCAT(@where, ' and ID_KhachHang =  @IDCustomer_In')
		end	
	if isnull(@LaHangHoa,2)!=2 --- 0.khong get xe thuoc DM_HangHoa, 1. chi get xe la HangHoa, 2.all
		begin
			if @LaHangHoa = 1
				set @where= CONCAT(@where, ' and hh.ID_Xe is not null')
			if @LaHangHoa = 0
				set @where= CONCAT(@where, ' and hh.ID_Xe is null') 
		end	
	--lọc sở hưu của gara hay khách hàng cho phần làm phiếu bàn giao xe
	if ISNULL(@NguoiSoHuu, 2) != 2
	begin
		if @NguoiSoHuu = 0
		begin
			set @where = CONCAT(@where, ' and (xe.NguoiSoHuu = 0 or xe.NguoiSoHuu is null)')
		end
		else if @NguoiSoHuu = 1
		begin
			set @where = CONCAT(@where, ' and xe.NguoiSoHuu = 1')
		end
	end

	set @sql= CONCAT(@sql, '  select top 30 xe.ID, xe.ID_MauXe, xe.ID_KhachHang, xe.BienSo, xe.SoKhung, xe.SoMay, xe.HopSo, xe.DungTich, xe.MauSon, xe.NamSanXuat
    	from Gara_DanhMucXe xe
		left join DM_HangHoa hh on xe.ID = hh.ID_Xe
		', @where)

		set @paramDefined = N' @TextSearch_In nvarchar(max), @IDCustomer_In nvarchar(max), @StatusTN_In nvarchar(max)'
    

--	print @sql
	

	exec sp_executesql @sql, @paramDefined,	
	@TextSearch_In = @TextSearch, 	
	@StatusTN_In = @StatusTN,
	@IDCustomer_In = @IDCustomer

   
END");

			Sql(@"ALTER PROCEDURE [dbo].[ListHangHoaTheKho]
    @ID_HangHoa [uniqueidentifier],
    @IDChiNhanh [uniqueidentifier]
AS
BEGIN
	SET NOCOUNT ON;
	SELECT ID_HoaDon, 
		MaHoaDon, 
		NgayLapHoaDon,
		LoaiHoaDon, 
		ID_DonVi,
		ID_CheckIn,		
		sum(table1.SoLuong) as SoLuong,
		max(table1.GiaVon) as GiaVon,
		max(table1.TonKho) as TonKho,
	case table1.LoaiHoaDon
			when 10 then case when table1.ID_CheckIn = @IDChiNhanh then N'Nhận chuyển hàng' else N'Chuyển hàng' end
			when 1 then N'Bán hàng'
			when 2 then N'Bảo hành'
			when 4 then N'Nhập hàng'
			when 6 then N'Khách trả hàng'
			when 7 then N'Trả hàng nhập'
			when 8 then N'Xuất kho'
			when 9 then N'Kiểm hàng'
			when 13 then N'Nhập nội bộ'
			when 14 then N'Nhập hàng thừa'
			when 18 then N'Điều chỉnh giá vốn'			
		end as LoaiChungTu
	FROM
	(
		SELECT hd.ID as ID_HoaDon, hd.MaHoaDon, hd.LoaiHoaDon, 
		CASE WHEN hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4' and hd.LoaiHoaDon = 10 THEN hd.NgaySua ELSE hd.NgayLapHoaDon END as NgayLapHoaDon,
		bhct.ThanhTien * dvqd.TyLeChuyenDoi as ThanhTien,
		bhct.TienChietKhau * dvqd.TyLeChuyenDoi TienChietKhau, 
		dvqd.TyLeChuyenDoi,
		hd.YeuCau, 
		hd.ID_CheckIn,
		hd.ID_DonVi,
		hh.QuanLyTheoLoHang,
		dvqd.LaDonViChuan, 
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.TonLuyKe, bhct.TonLuyKe_NhanChuyenHang) as TonKho,
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.GiaVon / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi),bhct.GiaVon_NhanChuyenHang / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi)) as GiaVon,	
		bhct.SoThuTu,
		(case hd.LoaiHoaDon
			when 9 then bhct.SoLuong ---- Số lượng lệch = SLThucTe - TonKhoDB        (-) Giảm  (+): Tăng
			when 10 then
				case when hd.ID_CheckIn= @IDChiNhanh and hd.YeuCau = '4' then bhct.TienChietKhau  ---- da nhanhang
				else bhct.SoLuong end 
			--- xuat
			when 1 then - bhct.SoLuong
			when 2 then - bhct.SoLuong
			when 7 then - bhct.SoLuong
			when 8 then - bhct.SoLuong
			--- conlai: nhap
			else bhct.SoLuong end
		) * dvqd.TyLeChuyenDoi as SoLuong
		
	FROM BH_HoaDon hd
	LEFT JOIN BH_HoaDon_ChiTiet bhct on hd.ID = bhct.ID_HoaDon
	LEFT JOIN DonViQuiDoi dvqd on bhct.ID_DonViQuiDoi = dvqd.ID
	LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
	WHERE hd.LoaiHoaDon not in (3 , 25,29,31)
	AND IIF(hd.LoaiHoaDon != 6, 1, 
	IIF(bhct.ChatLieu != '2' OR bhct.ChatLieu IS NULL, 1, 0)) = 1 
	and hd.LoaiHoaDon != 19 and hh.ID = @ID_HangHoa and hd.ChoThanhToan = 0 
	and ((hd.ID_DonVi = @IDChiNhanh and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null)) or (hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4'))
	)  table1
	group by ID_HoaDon, MaHoaDon, NgayLapHoaDon,LoaiHoaDon, ID_DonVi, ID_CheckIn
	ORDER BY NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[ListHangHoaTheKhoTheoLoHang]
    @ID_HangHoa [uniqueidentifier],
    @IDChiNhanh [uniqueidentifier],
    @ID_LoHang [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;

	SELECT ID_HoaDon, 
		MaHoaDon, 
		NgayLapHoaDon,
		LoaiHoaDon, 
		ID_DonVi,
		ID_CheckIn,		
		sum(table1.SoLuong) as SoLuong,
		max(table1.GiaVon) as GiaVon,
		max(table1.TonKho) as TonKho,
		case table1.LoaiHoaDon
			when 10 then case when table1.ID_CheckIn = @IDChiNhanh then N'Nhận chuyển hàng' else N'Chuyển hàng' end
			when 1 then N'Bán hàng'
			when 2 then N'Bảo hành'
			when 4 then N'Nhập hàng'
			when 6 then N'Khách trả hàng'
			when 7 then N'Trả hàng nhập'
			when 8 then N'Xuất kho'
			when 9 then N'Kiểm hàng'
			when 13 then N'Nhập kho nội bộ'
			when 14 then N'Nhập hàng khách thừa'
			when 18 then N'Điều chỉnh giá vốn'
		end as LoaiChungTu
		FROM
	(
    	SELECT hd.ID as ID_HoaDon, 
		hd.MaHoaDon, 
		hd.LoaiHoaDon, 
		hd.YeuCau, 
		hd.ID_CheckIn, 
		hd.ID_DonVi, 
		bhct.ThanhTien * dvqd.TyLeChuyenDoi as ThanhTien,
		bhct.TienChietKhau * dvqd.TyLeChuyenDoi TienChietKhau, 
		CASE WHEN hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4' and hd.LoaiHoaDon = 10 THEN hd.NgaySua ELSE hd.NgayLapHoaDon END as NgayLapHoaDon,    		
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.TonLuyKe, bhct.TonLuyKe_NhanChuyenHang) as TonKho,
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.GiaVon / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi),bhct.GiaVon_NhanChuyenHang / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi)) as GiaVon,	
		(case hd.LoaiHoaDon
			when 9 then bhct.SoLuong ---- Số lượng lệch = SLThucTe - TonKhoDB        (-) Giảm  (+): Tăng
			when 10 then
				case when hd.ID_CheckIn= @IDChiNhanh and hd.YeuCau = '4' then bhct.TienChietKhau  ---- da nhanhang
				else bhct.SoLuong end 
			--- xuat
			when 1 then - bhct.SoLuong
			when 2 then - bhct.SoLuong ---- hoadon baohanh
			when 7 then - bhct.SoLuong
			when 8 then - bhct.SoLuong
			--- conlai: nhap
			else bhct.SoLuong end
		) * dvqd.TyLeChuyenDoi as SoLuong
    	FROM BH_HoaDon hd
    	LEFT JOIN BH_HoaDon_ChiTiet bhct on hd.ID = bhct.ID_HoaDon
    	LEFT JOIN DonViQuiDoi dvqd on bhct.ID_DonViQuiDoi = dvqd.ID
    	LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID		
    	WHERE bhct.ID_LoHang = @ID_LoHang 
		and hd.LoaiHoaDon not in (3,19,25,31) 
		and hh.ID = @ID_HangHoa 
		and hd.ChoThanhToan = 0 
		and (bhct.ChatLieu is null or bhct.ChatLieu!='2')
		and ((hd.ID_DonVi = @IDChiNhanh 
		and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null)) or (hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4'))
	) as table1
    group by ID_HoaDon, MaHoaDon, NgayLapHoaDon,LoaiHoaDon, ID_DonVi, ID_CheckIn
	ORDER BY NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[LoadDanhMucHangHoa]
    @MaHH [nvarchar](max),
    @MaHHCoDau [nvarchar](max),
    @ListID_NhomHang [nvarchar](max),
    @ID_ChiNhanh [uniqueidentifier],
    @KinhDoanhFilter [nvarchar](max),
    @LoaiHangHoas [nvarchar](max),
    @List_ThuocTinh [nvarchar](max)
AS
BEGIN
	SET NOCOUNT ON;
    DECLARE @timeStart Datetime
    DECLARE @SQL VARCHAR(254)
    	DECLARE @tablename TABLE(
    Name [nvarchar](max))
    	DECLARE @tablenameChar TABLE(
    Name [nvarchar](max))
    	DECLARE @count int
    	DECLARE @countChar int
    	INSERT INTO @tablename(Name) select  Name from [dbo].[splitstring](@MaHH+',') where Name!='';
    		INSERT INTO @tablenameChar(Name) select  Name from [dbo].[splitstring](@MaHHCoDau+',') where Name!='';
    			  Select @count =  (Select count(*) from @tablename);
    	    Select @countChar =   (Select count(*) from @tablenameChar);

			declare @tblLoaiHang table(LoaiHang int)
			insert into @tblLoaiHang
			select name from dbo.splitstring(@LoaiHangHoas)

    if(@MaHH = '' and @MaHHCoDau = '')
    BEGIN
		
		if(@List_ThuocTinh != '')
		BEGIN
			select *,
			case LoaiHangHoa 
				when 1 then N'Hàng hóa'
				when 2 then N'Dịch vụ'
				when 3 then N'Combo'
			end as sLoaiHangHoa

			into #dmhanghoatable1
			from
			(
			select dvqd.ID as ID_DonViQuiDoi, hh.ID, hh.TonToiThieu, hh.TonToiDa, hh.GhiChu, hh.LaHangHoa, hh.QuanLyTheoLoHang,hhtt.GiaTri + CAST(hhtt.ID_ThuocTinh AS NVARCHAR(36)) AS ThuocTinh, hh.LaChaCungLoai,
			hh.DuocBanTrucTiep, hh.TheoDoi as TrangThai, hh.NgayTao, hh.ID_HangHoaCungLoai, dvqd.MaHangHoa, hh.ID_NhomHang as ID_NhomHangHoa, 
			dnhh.TenNhomHangHoa as NhomHangHoa , hh.TenHangHoa, dvqd.TenDonViTinh, dvqd.ThuocTinhGiaTri,
			isnull(hh.SoPhutThucHien,0) as SoPhutThucHien,
			isnull(hh.DichVuTheoGio,0) as DichVuTheoGio,
			isnull(hh.DuocTichDiem,0) as DuocTichDiem,
			isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
			isnull(hh.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT,
			iif(hh.QuanLyBaoDuong is null,0, hh.QuanLyBaoDuong) as QuanLyBaoDuong,
			iif(hh.LoaiBaoDuong is null,0, hh.LoaiBaoDuong) as LoaiBaoDuong,
			iif(hh.SoKmBaoHanh is null,0, hh.SoKmBaoHanh) as SoKmBaoHanh,	
			iif(hh.HoaHongTruocChietKhau is null,0, hh.HoaHongTruocChietKhau) as HoaHongTruocChietKhau,		
			iif(hh.LoaiHangHoa is null,iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,
			iif(hh.LaHangHoa='1', isnull(gv.GiaVon,0), dbo.GetGiaVonOfDichVu(@ID_ChiNhanh,dvqd.ID)) as GiaVon,					
			dvqd.GiaBan,
			dvqd.Xoa,
			ISNULL(hhtonkho.TonKho,0) as TonKho			
			from DonViQuiDoi dvqd
			LEFT JOIN DM_HangHoa_TonKho hhtonkho on dvqd.ID = hhtonkho.ID_DonViQuyDoi and hhtonkho.ID_DonVi = @ID_ChiNhanh
			LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
			LEFT JOIN HangHoa_ThuocTinh hhtt on hh.ID = hhtt.ID_HangHoa
    		LEFT JOIN DM_NhomHangHoa dnhh ON dnhh.ID = hh.ID_NhomHang
    		LEFT JOIN DM_GiaVon gv on dvqd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi = @ID_ChiNhanh and gv.ID_LoHang is null
			where dvqd.ladonvichuan = 1 and ((select TOP 1 [name] from splitstring(@ListID_NhomHang) ORDER BY [name]) = '' or dnhh.id=(select * from splitstring(@ListID_NhomHang) where [name] like dnhh.ID))
			and hh.TheoDoi like @KinhDoanhFilter
			) a where exists (select LoaiHang from @tblLoaiHang loai where a.LoaiHangHoa = loai.LoaiHang)

			Select * from #dmhanghoatable1 hhtb2
    		where ThuocTinh COLLATE Latin1_General_CI_AI in (select [name] COLLATE Latin1_General_CI_AI from splitstring(@List_ThuocTinh))
		END
		ELSE
		BEGIN
			select *,
			case LoaiHangHoa 
				when 1 then N'Hàng hóa'
				when 2 then N'Dịch vụ'
				when 3 then N'Combo'
			end as sLoaiHangHoa
			into #dmhanghoatable2
			from
			(
				select dvqd.ID as ID_DonViQuiDoi, hh.ID, hh.TonToiThieu, hh.TonToiDa, hh.GhiChu, hh.LaHangHoa, hh.QuanLyTheoLoHang, hh.LaChaCungLoai,
				hh.DuocBanTrucTiep, hh.TheoDoi as TrangThai, hh.NgayTao, hh.ID_HangHoaCungLoai, dvqd.MaHangHoa, hh.ID_NhomHang as ID_NhomHangHoa, 
				dnhh.TenNhomHangHoa as NhomHangHoa , hh.TenHangHoa, dvqd.TenDonViTinh, dvqd.ThuocTinhGiaTri,
				isnull(hh.SoPhutThucHien,0) as SoPhutThucHien,
				isnull(hh.DichVuTheoGio,0) as DichVuTheoGio,
				isnull(hh.DuocTichDiem,0) as DuocTichDiem,
				isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
				isnull(hh.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT,
				iif(hh.QuanLyBaoDuong is null,0, hh.QuanLyBaoDuong) as QuanLyBaoDuong,
				iif(hh.LoaiBaoDuong is null,0, hh.LoaiBaoDuong) as LoaiBaoDuong,
				iif(hh.SoKmBaoHanh is null,0, hh.SoKmBaoHanh) as SoKmBaoHanh,
				iif(hh.HoaHongTruocChietKhau is null,0, hh.HoaHongTruocChietKhau) as HoaHongTruocChietKhau,		
				iif(hh.LoaiHangHoa is null,iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,
				iif(hh.LaHangHoa='1', isnull(gv.GiaVon,0), dbo.GetGiaVonOfDichVu(@ID_ChiNhanh,dvqd.ID)) as GiaVon,			
				dvqd.GiaBan, 
				dvqd.Xoa,
				ISNULL(hhtonkho.TonKho,0) as TonKho ,
				hh.ID_Xe,
				xe.BienSo
				from DonViQuiDoi dvqd
				LEFT JOIN DM_HangHoa_TonKho hhtonkho on dvqd.ID = hhtonkho.ID_DonViQuyDoi and hhtonkho.ID_DonVi = @ID_ChiNhanh
				LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    			LEFT JOIN DM_NhomHangHoa dnhh ON dnhh.ID = hh.ID_NhomHang
    			LEFT JOIN DM_GiaVon gv on dvqd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi = @ID_ChiNhanh and gv.ID_LoHang is null
				left join Gara_DanhMucXe xe on hh.ID_Xe= xe.ID
				where  dvqd.ladonvichuan = 1 and ((select TOP 1 [name] from splitstring(@ListID_NhomHang) ORDER BY [name]) = '' or dnhh.id=(select * from splitstring(@ListID_NhomHang) where [name] like dnhh.ID))
				and hh.TheoDoi like @KinhDoanhFilter
				) a  where exists (select LoaiHang from @tblLoaiHang loai where a.LoaiHangHoa = loai.LoaiHang)

			Select * from #dmhanghoatable2 hhtb2
		END

    END

    if(@MaHH != '' or @MaHHCoDau != '')
    BEGIN
    	if(@List_ThuocTinh != '')
		BEGIN
			select *,
				case LoaiHangHoa 
					when 1 then N'Hàng hóa'
					when 2 then N'Dịch vụ'
					when 3 then N'Combo'
				end as sLoaiHangHoa
			 into #dmhanghoatable3
			from
			(
				select dvqd.ID as ID_DonViQuiDoi, hh.ID, hh.TonToiThieu, hh.TonToiDa, hh.GhiChu, hh.LaHangHoa, hh.QuanLyTheoLoHang,hhtt.GiaTri + CAST(hhtt.ID_ThuocTinh AS NVARCHAR(36)) AS ThuocTinh, hh.LaChaCungLoai,
				hh.DuocBanTrucTiep, hh.TheoDoi as TrangThai, hh.NgayTao, hh.ID_HangHoaCungLoai, dvqd.MaHangHoa, hh.ID_NhomHang as ID_NhomHangHoa, 
				dnhh.TenNhomHangHoa as NhomHangHoa , hh.TenHangHoa, dvqd.TenDonViTinh, dvqd.ThuocTinhGiaTri,
				isnull(hh.SoPhutThucHien,0) as SoPhutThucHien,
				isnull(hh.DichVuTheoGio,0) as DichVuTheoGio,	
				isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
				isnull(hh.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT,
				isnull(hh.DuocTichDiem,0) as DuocTichDiem,
				iif(hh.QuanLyBaoDuong is null,0, hh.QuanLyBaoDuong) as QuanLyBaoDuong,
				iif(hh.LoaiBaoDuong is null,0, hh.LoaiBaoDuong) as LoaiBaoDuong,
				iif(hh.SoKmBaoHanh is null,0, hh.SoKmBaoHanh) as SoKmBaoHanh,
				iif(hh.HoaHongTruocChietKhau is null,0, hh.HoaHongTruocChietKhau) as HoaHongTruocChietKhau,		
				iif(hh.LoaiHangHoa is null,iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,
				iif(hh.LaHangHoa='1', isnull(gv.GiaVon,0), dbo.GetGiaVonOfDichVu(@ID_ChiNhanh,dvqd.ID)) as GiaVon,				
				dvqd.GiaBan, ISNULL(hhtonkho.TonKho,0) as TonKho,
				dvqd.Xoa,
				hh.ID_Xe,
				xe.BienSo
				FROM DonViQuiDoi dvqd
				LEFT JOIN DM_HangHoa_TonKho hhtonkho on dvqd.ID = hhtonkho.ID_DonViQuyDoi and hhtonkho.ID_DonVi = @ID_ChiNhanh
				LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
				LEFT JOIN HangHoa_ThuocTinh hhtt on hh.ID = hhtt.ID_HangHoa
    			LEFT JOIN DM_NhomHangHoa dnhh ON dnhh.ID = hh.ID_NhomHang
    			LEFT JOIN DM_GiaVon gv on dvqd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi = @ID_ChiNhanh and gv.ID_LoHang is null
				left join Gara_DanhMucXe xe on hh.ID_Xe= xe.ID
    			where dvqd.ladonvichuan = 1 and
    			((select count(*) from @tablename b where 
    			hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
				or hh.TenHangHoa like '%'+b.Name+'%'
				or hh.GhiChu like '%' +b.Name +'%' 
				or xe.BienSo like '%' +b.Name +'%' 
    			or dvqd.MaHangHoa like '%'+b.Name+'%' )=@count or @count=0)
    			and ((select count(*) from @tablenameChar c where
    			hh.TenHangHoa like '%'+c.Name+'%' or hh.GhiChu like '%'+c.Name+'%'
    			or dvqd.MaHangHoa like '%'+c.Name+'%' )= @countChar or @countChar=0)
    			 and ((select TOP 1 [name] from splitstring(@ListID_NhomHang) ORDER BY [name]) = '' or dnhh.id=(select * from splitstring(@ListID_NhomHang) where [name] like dnhh.ID))
    			and hh.TheoDoi like @KinhDoanhFilter
				) a where exists (select LoaiHang from @tblLoaiHang loai where a.LoaiHangHoa = loai.LoaiHang)

			Select * from #dmhanghoatable3 hhtb2	
    				where ThuocTinh COLLATE Latin1_General_CI_AI in (select [name] COLLATE Latin1_General_CI_AI from splitstring(@List_ThuocTinh))
		END
		ELSE
		BEGIN
		select *,
			case LoaiHangHoa 
				when 1 then N'Hàng hóa'
				when 2 then N'Dịch vụ'
				when 3 then N'Combo'
			end as sLoaiHangHoa
			 into #dmhanghoatable4
			from
			(
			select dvqd.ID as ID_DonViQuiDoi, hh.ID, hh.TonToiThieu, hh.TonToiDa, hh.GhiChu, hh.LaHangHoa, hh.QuanLyTheoLoHang, hh.LaChaCungLoai,
			hh.DuocBanTrucTiep, hh.TheoDoi as TrangThai, hh.NgayTao, hh.ID_HangHoaCungLoai, dvqd.MaHangHoa, hh.ID_NhomHang as ID_NhomHangHoa, 
			dnhh.TenNhomHangHoa as NhomHangHoa , hh.TenHangHoa, dvqd.TenDonViTinh, dvqd.ThuocTinhGiaTri,
			isnull(hh.SoPhutThucHien,0) as SoPhutThucHien,
			isnull(hh.DichVuTheoGio,0) as DichVuTheoGio,
			isnull(hh.DuocTichDiem,0) as DuocTichDiem,
			isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
			isnull(hh.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT,
			iif(hh.QuanLyBaoDuong is null,0, hh.QuanLyBaoDuong) as QuanLyBaoDuong,
			iif(hh.LoaiBaoDuong is null,0, hh.LoaiBaoDuong) as LoaiBaoDuong,
			iif(hh.SoKmBaoHanh is null,0, hh.SoKmBaoHanh) as SoKmBaoHanh,
			iif(hh.HoaHongTruocChietKhau is null,0, hh.HoaHongTruocChietKhau) as HoaHongTruocChietKhau,		
			iif(hh.LoaiHangHoa is null,iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,
			iif(hh.LaHangHoa='1', isnull(gv.GiaVon,0), dbo.GetGiaVonOfDichVu(@ID_ChiNhanh,dvqd.ID)) as GiaVon,
			dvqd.GiaBan, ISNULL(hhtonkho.TonKho,0) as TonKho ,
			dvqd.Xoa,
			hh.ID_Xe,
			xe.BienSo
			FROM DonViQuiDoi dvqd
			LEFT JOIN DM_HangHoa_TonKho hhtonkho on dvqd.ID = hhtonkho.ID_DonViQuyDoi and hhtonkho.ID_DonVi = @ID_ChiNhanh 
			LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		LEFT JOIN DM_NhomHangHoa dnhh ON dnhh.ID = hh.ID_NhomHang
    		LEFT JOIN DM_GiaVon gv on dvqd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi = @ID_ChiNhanh and gv.ID_LoHang is null
			left join Gara_DanhMucXe xe on hh.ID_Xe= xe.ID
    		where dvqd.ladonvichuan = 1 and
    		((select count(*) from @tablename b where 
    		hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
			or hh.TenHangHoa like '%'+b.Name+'%'
			or hh.GhiChu like '%' +b.Name +'%' 
			or xe.BienSo like '%' +b.Name +'%' 
    		or dvqd.MaHangHoa like '%'+b.Name+'%' )=@count or @count=0)
    		and ((select count(*) from @tablenameChar c where
    		hh.TenHangHoa like '%'+c.Name+'%' or hh.GhiChu like '%'+c.Name+'%'
    		or dvqd.MaHangHoa like '%'+c.Name+'%' )= @countChar or @countChar=0)
    		 and ((select TOP 1 [name] from splitstring(@ListID_NhomHang) ORDER BY [name]) = '' or dnhh.id=(select * from splitstring(@ListID_NhomHang) where [name] like dnhh.ID))
    		and hh.TheoDoi like @KinhDoanhFilter 
			) a where exists (select LoaiHang from @tblLoaiHang loai where a.LoaiHangHoa = loai.LoaiHang)

			Select * from #dmhanghoatable4 hhtb2	
		END
	END
END");

			Sql(@"ALTER PROCEDURE [dbo].[LoadGiaBanChiTiet]
    @ID_ChiNhanh [uniqueidentifier],
    @ID_BangGia [nvarchar](max),
    @maHoaDon [nvarchar](max),
    @maHoaDonVie [nvarchar](max)
AS
BEGIN
    DECLARE @tablename TABLE(
    Name [nvarchar](max))
    	DECLARE @tablenameChar TABLE(
    Name [nvarchar](max))
    	DECLARE @count int
    	DECLARE @countChar int
    	INSERT INTO @tablename(Name) select  Name from [dbo].[splitstring](@maHoaDon+',') where Name!='';
    	INSERT INTO @tablenameChar(Name) select  Name from [dbo].[splitstring](@maHoaDonVie+',') where Name!='';
    	Select @count =  (Select count(*) from @tablename);
    	Select @countChar =   (Select count(*) from @tablenameChar);
    
    if(@ID_BangGia != '')
    	BEGIN
    		Select gbct.ID, dvqd.ID_HangHoa,hh.TenHangHoa,
    			concat(hh.TenHangHoa , dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
				dvqd.ThuocTinhGiaTri as HangHoaThuocTinh,
    			hh.QuanLyTheoLoHang, hh.NgayTao,dvqd.Xoa, hh.ID_NhomHang, dvqd.TenDonViTinh as DonViTinh, dvqd.GiaNhap as GiaNhapCuoi, gbct.GiaBan as GiaMoi, dvqd.MaHangHoa,
    		ISNULL(CAST(gv.GiaVon as FLOAT), 0) as GiaVon, dvqd.GiaBan, dvqd.GiaBan as GiaChung, dvqd.ID as IDQuyDoi, gbct.ID_GiaBan, nhh.TenNhomHangHoa
    		from DonViQuiDoi dvqd
    		left join DM_GiaBan_ChiTiet gbct on dvqd.ID = gbct.ID_DonViQuiDoi
    		left join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    			LEFT JOIN DM_GiaVon gv on dvqd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi = @ID_ChiNhanh and gv.ID_LoHang is null
    		left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID
    		where dvqd.Xoa = 0 and hh.TheoDoi =1 and gbct.ID_Giaban = @ID_BangGia
			and ((select count(*) from @tablename b where 
    					hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    						or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
							or hh.TenHangHoa like '%'+b.Name+'%' 
    						or dvqd.MaHangHoa like '%'+b.Name+'%' )=@count or @count=0)
    					and ((select count(*) from @tablenameChar c where
    						hh.TenHangHoa like '%'+c.Name+'%' 
    						or dvqd.MaHangHoa like '%'+c.Name+'%' )= @countChar or @countChar=0)
			 
    		order by gbct.NgayNhap desc
    	END
    	ELSE
    	BEGIN
    		Select dvqd.ID, dvqd.ID_HangHoa,hh.TenHangHoa,
    			concat(hh.TenHangHoa , dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
				dvqd.ThuocTinhGiaTri as HangHoaThuocTinh,
    			hh.QuanLyTheoLoHang,hh.NgayTao,dvqd.Xoa, hh.ID_NhomHang, dvqd.TenDonViTinh as DonViTinh, dvqd.GiaNhap as GiaNhapCuoi, dvqd.GiaBan as GiaMoi, dvqd.MaHangHoa,
    		ISNULL(CAST(gv.GiaVon as FLOAT), 0) as GiaVon, dvqd.GiaBan, dvqd.ID as IDQuyDoi, NEWID() as ID_GiaBan, nhh.TenNhomHangHoa
    		from DonViQuiDoi dvqd
    		left join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    			LEFT JOIN DM_GiaVon gv on dvqd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi = @ID_ChiNhanh and gv.ID_LoHang is null
    			left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID 
    				
    		where ((select count(*) from @tablename b where 
    					hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    						or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
							or hh.TenHangHoa like '%'+b.Name+'%' 
    						or dvqd.MaHangHoa like '%'+b.Name+'%' )=@count or @count=0)
    					and ((select count(*) from @tablenameChar c where
    						hh.TenHangHoa like '%'+c.Name+'%' 
    						or dvqd.MaHangHoa like '%'+c.Name+'%' )= @countChar or @countChar=0)
    			and dvqd.Xoa = 0 and hh.TheoDoi =1
    		order by hh.NgayTao desc	
    		END
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportDiscountInvoice]
    @ID_ChiNhanhs [nvarchar](max),
    @ID_NhanVienLogin [nvarchar](max),
    @TextSearch [nvarchar](max),
	@LoaiChungTus [nvarchar](max),
    @DateFrom [nvarchar](max),
    @DateTo [nvarchar](max),
    @Status_ColumHide [int],
    @StatusInvoice [int],
    @Status_DoanhThu [int],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    	set @DateTo = dateadd(day,1, @DateTo) 

		declare @tblChiNhanh table (ID uniqueidentifier)
    	insert into @tblChiNhanh
    	select * from dbo.splitstring(@ID_ChiNhanhs)
    
    	declare @nguoitao nvarchar(100) = (select top 1 taiKhoan from HT_NguoiDung where ID_NhanVien= @ID_NhanVienLogin)
    	declare @tblNhanVien table (ID uniqueidentifier)
    	insert into @tblNhanVien
    	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanhs,'BCCKHoaDon_XemDS_PhongBan','BCCKHoaDon_XemDS_HeThong');

		declare @tblChungTu table (LoaiChungTu int)
    	insert into @tblChungTu
    	select Name from dbo.splitstring(@LoaiChungTus)
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	declare @tblDiscountInvoice table (ID uniqueidentifier, MaNhanVien nvarchar(50), TenNhanVien nvarchar(max), 
    		HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float)
    	
    	-- bang tam chua DS phieu thu theo Ngay timkiem
    	select qct.ID_HoaDonLienQuan, SUM(qct.TienThu) as ThucThu, qhd.NgayLapHoaDon, qhd.ID
    	into #temp
    	from Quy_HoaDon_ChiTiet qct
    	join (
    			select qhd.ID, qhd.NgayLapHoaDon
    			from Quy_HoaDon qhd
    			join BH_NhanVienThucHien th on qhd.ID= th.ID_QuyHoaDon
    			where (qhd.TrangThai is null or qhd.TrangThai = '1')
    			and qhd.ID_DonVi in (select ID from @tblChiNhanh)
    			and qhd.NgayLapHoaDon >= @DateFrom
    			and qhd.NgayLapHoaDon < @DateTo
    			group by qhd.ID, qhd.NgayLapHoaDon) qhd on qct.ID_HoaDon = qhd.ID
    	where (qct.HinhThucThanhToan is null or qct.HinhThucThanhToan != 4)
    	group by qct.ID_HoaDonLienQuan, qhd.NgayLapHoaDon, qhd.ID;
    	
    	select ID, MaNhanVien, 
    			TenNhanVien,
    		SUM(ISNULL(HoaHongThucThu,0.0)) as HoaHongThucThu,
    		SUM(ISNULL(HoaHongDoanhThu,0.0)) as HoaHongDoanhThu,
    		SUM(ISNULL(HoaHongVND,0.0)) as HoaHongVND,			
    		case @Status_ColumHide
    			when  1 then cast(0 as float)
    			when  2 then SUM(ISNULL(HoaHongVND,0.0))
    			when  3 then SUM(ISNULL(HoaHongThucThu,0.0))
    			when  4 then SUM(ISNULL(HoaHongThucThu,0.0)) + SUM(ISNULL(HoaHongVND,0.0))
    			when  5 then SUM(ISNULL(HoaHongDoanhThu,0.0)) 
    			when  6 then SUM(ISNULL(HoaHongDoanhThu,0.0)) + SUM(ISNULL(HoaHongVND,0.0))
    			when  7 then SUM(ISNULL(HoaHongThucThu,0.0)) + SUM(ISNULL(HoaHongDoanhThu,0.0))
    		else SUM(ISNULL(HoaHongThucThu,0.0)) + SUM(ISNULL(HoaHongDoanhThu,0.0)) + SUM(ISNULL(HoaHongVND,0.0))
    		end as TongAll
    		into #temp2
    	from 
    	(
    		select nv.ID, MaNhanVien, TenNhanVien,
    			case when TinhChietKhauTheo =1 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau  end end as HoaHongThucThu,
    				case when TinhChietKhauTheo =3 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau end end as HoaHongVND,
    				-- neu HD tao thang truoc, nhung PhieuThu thuoc thang nay: HoaHongDoanhThu = 0
    				case when hd.NgayLapHoaDon between @DateFrom and @DateTo and hd.ID_DonVi in (select ID from @tblChiNhanh) then
    					case when TinhChietKhauTheo = 2 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau end end else 0 end as HoaHongDoanhThu,
    				-- timkiem theo NgayLapHD or NgayLapPhieuThu
    				case when @DateFrom <= hd.NgayLapHoaDon and  hd.NgayLapHoaDon < @DateTo then hd.NgayLapHoaDon else tblQuy.NgayLapHoaDon end as NgayLapHoaDon,
    				case when hd.ChoThanhToan='0' then 1 else 2 end as TrangThaiHD
    		from BH_NhanVienThucHien th
    		join BH_HoaDon hd on th.ID_HoaDon= hd.ID
    		join NS_NhanVien nv on th.ID_NhanVien= nv.ID
    			left join #temp tblQuy on hd.ID= tblQuy.ID_HoaDonLienQuan and (th.ID_QuyHoaDon= tblQuy.ID)	
    		where th.ID_HoaDon is not null
    		and hd.ChoThanhToan  is not null
    			and hd.ID_DonVi in (select * from dbo.splitstring(@ID_ChiNhanhs))  
				and (exists (select LoaiChungTu from @tblChungTu ctu where ctu.LoaiChungTu = hd.LoaiHoaDon))
    			and (exists (select ID from @tblNhanVien nv where th.ID_NhanVien = nv.ID) or hd.NguoiTao like @nguoitao)
    			-- chi lay CKDoanhThu hoac CKThucThu/VND exist in Quy_HoaDon or (not exist QuyHoaDon but LoaiHoaDon =6 )
    			and (th.TinhChietKhauTheo != 1 or (th.TinhChietKhauTheo =1 and ( exists (select ID from #temp where th.ID_QuyHoaDon = #temp.ID) or  LoaiHoaDon=6)))
    			and
    				((select count(Name) from @tblSearchString b where     			
    				nv.TenNhanVien like '%'+b.Name+'%'
    				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%'+b.Name+'%'				
    				)=@count or @count=0)	
    	) tbl
    		where tbl.NgayLapHoaDon >= @DateFrom and tbl.NgayLapHoaDon < @DateTo and TrangThaiHD = @StatusInvoice
    	group by MaNhanVien, TenNhanVien, ID
    		having SUM(ISNULL(HoaHongThucThu,0)) + SUM(ISNULL(HoaHongDoanhThu,0)) + SUM(ISNULL(HoaHongVND,0)) > 0 -- chi lay NV co CK > 0
    		
    		if @Status_DoanhThu =0
    			insert into @tblDiscountInvoice
    			select *
    			from #temp2 
    		else
    			begin
    				if @Status_DoanhThu= 1
    					insert into @tblDiscountInvoice
    					select *
    					from #temp2 where HoaHongDoanhThu > 0 or HoaHongThucThu > 0
    				else
    					if @Status_DoanhThu= 2
    						insert into @tblDiscountInvoice
    						select *
    						from #temp2 where HoaHongDoanhThu > 0 or HoaHongVND > 0
    					else		
    						if @Status_DoanhThu= 3
    							insert into @tblDiscountInvoice
    							select *
    							from #temp2 where HoaHongDoanhThu > 0
    						else	
    							if @Status_DoanhThu= 4
    								insert into @tblDiscountInvoice
    								select *
    								from #temp2 where HoaHongVND > 0 Or HoaHongThucThu > 0
    							else
    								if @Status_DoanhThu= 5
    									insert into @tblDiscountInvoice
    									select *
    									from #temp2 where  HoaHongThucThu > 0
    								else -- 6
    									insert into @tblDiscountInvoice
    									select *
    									from #temp2  where HoaHongVND > 0
    								
    			end;
    			
    		with data_cte
    		as(
    		select * from @tblDiscountInvoice
    		),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
    				sum(HoaHongDoanhThu) as TongHoaHongDoanhThu,
    				sum(HoaHongThucThu) as TongHoaHongThucThu,
    				sum(HoaHongVND) as TongHoaHongVND,
    				sum(TongAll) as TongAllAll
    			from data_cte
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.MaNhanVien
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportDiscountInvoice_Detail]
    @ID_ChiNhanhs [nvarchar](max),
    @ID_NhanVienLogin [nvarchar](max),
    @TextSearch [nvarchar](max),
	@LoaiChungTus [nvarchar](max),
    @DateFrom [nvarchar](max),
    @DateTo [nvarchar](max),
    @Status_ColumHide [int],
    @StatusInvoice [int],
    @Status_DoanhThu [int],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;

		declare @tblChiNhanh table (ID uniqueidentifier)
    	insert into @tblChiNhanh
    	select * from dbo.splitstring(@ID_ChiNhanhs)
    
    	declare @tblNhanVien table (ID uniqueidentifier)
    	insert into @tblNhanVien
    	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanhs,'BCCKHoaDon_XemDS_PhongBan','BCCKHoaDon_XemDS_HeThong');

		declare @tblChungTu table (LoaiChungTu int)
    	insert into @tblChungTu
    	select Name from dbo.splitstring(@LoaiChungTus)
    
    	set @DateTo = DATEADD(day,1,@DateTo)
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
		DECLARE @count int;
		INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
		Select @count =  (Select count(*) from @tblSearchString);
    
    	declare @tblDiscountInvoice table (ID uniqueidentifier, MaNhanVien nvarchar(50), TenNhanVien nvarchar(max), NgayLapHoaDon datetime, NgayLapPhieu datetime, NgayLapPhieuThu datetime, MaHoaDon nvarchar(50),
    		DoanhThu float, ThucThu float, HeSo float, HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, PTThucThu float, PTDoanhThu float, 
    		MaKhachHang nvarchar(max), TenKhachHang nvarchar(max), DienThoaiKH nvarchar(max), TongAll float)
    
  --  	-- bang tam chua DS phieu thu theo Ngay timkiem
		select qct.ID_HoaDonLienQuan, 
			qhd.ID,
			qhd.NgayLapHoaDon, 
			SUM(iif(qhd.LoaiHoaDon = 11, qct.TienThu, -qct.TienThu)) as ThucThu,
			max(isnull(qct.ChiPhiNganHang,0)) as ChiPhiNganHang,
			isnull(qct.LaPTChiPhiNganHang,'1') as LaPTChiPhiNganHang
    	into #tempQuy
    	from Quy_HoaDon_ChiTiet qct
		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID 
		where (qhd.TrangThai is null or qhd.TrangThai = '1')
		and qhd.ID_DonVi in (select ID from @tblChiNhanh)
		and qhd.NgayLapHoaDon >= @DateFrom
    	and qhd.NgayLapHoaDon < @DateTo 
		and qct.HinhThucThanhToan not in (4,5)
    	group by  qct.ID_HoaDonLienQuan, qhd.NgayLapHoaDon, qhd.ID, qct.LaPTChiPhiNganHang

		---- thucthu theo hoadon
		select ctquy.*, tblTong.TongThuThucTe
		into #tblQuyThucTe
		from #tempQuy ctquy
		left join
		(
		select ID_HoaDonLienQuan,		
			sum(ThucThu) as TongThuThucTe
		from #tempQuy
		group by ID_HoaDonLienQuan
		) tblTong on ctquy.ID_HoaDonLienQuan= tblTong.ID_HoaDonLienQuan
		
    
    		select
				tbl.ID, ---- id of hoadon
				MaNhanVien, 
    			tbl.TenNhanVien,
    			tbl.NgayLapHoaDon,
    			tbl.NgayLapPhieu, -- used to check at where condition
    			tbl.NgayLapPhieuThu,
    			tbl.MaHoaDon,		
				
    			-- taoHD truoc, PhieuThu sau --> khong co doanh thu
    			case when  tbl.NgayLapHoaDon between @DateFrom and @DateTo then PhaiThanhToan else 0 end as DoanhThu, 
    			ISNULL(ThucThu,0) as ThucThu,
    			ISNULL(HeSo,0) as HeSo,
    			ISNULL(HoaHongThucThu,0) as HoaHongThucThu,
    			ISNULL(HoaHongDoanhThu,0) as HoaHongDoanhThu,
    			ISNULL(HoaHongVND,0) as HoaHongVND,
    			ISNULL(PTThucThu,0) as PTThucThu,
    			ISNULL(PTDoanhThu,0) as PTDoanhThu,
    			ISNULL(MaDoiTuong,'') as MaKhachHang,
    			ISNULL(TenDoiTuong,N'Khách lẻ') as TenKhachHang,
    			ISNULL(dt.DienThoai,'') as DienThoaiKH,		
    		case @Status_ColumHide
    			when  1 then cast(0 as float)
    			when  2 then ISNULL(HoaHongVND,0.0)
    			when  3 then ISNULL(HoaHongThucThu,0.0)
    			when  4 then ISNULL(HoaHongThucThu,0.0) + ISNULL(HoaHongVND,0.0)
    			when  5 then ISNULL(HoaHongDoanhThu,0.0) 
    			when  6 then ISNULL(HoaHongDoanhThu,0.0) + ISNULL(HoaHongVND,0.0)
    			when  7 then ISNULL(HoaHongThucThu,0.0) + ISNULL(HoaHongDoanhThu,0.0)
    		else ISNULL(HoaHongThucThu,0.0) + ISNULL(HoaHongDoanhThu,0.0) + ISNULL(HoaHongVND,0.0)
    		end as TongAll
    		into #temp2
    	from 
    	(    		
    				select distinct MaNhanVien, TenNhanVien, hd.MaHoaDon, 
    					case when hd.LoaiHoaDon= 6 then - TongThanhToan + isnull(TongTienThue,0)
    					else case when hd.ID_DonVi in (select ID from @tblChiNhanh) then
							iif(hd.LoaiHoaDon=22, PhaiThanhToan, TongThanhToan - TongTienThue) else 0 end end as PhaiThanhToan,
    					hd.NgayLapHoaDon,
						tblQuy.ThucThu ,	
						tblQuy.ChiPhiNganHang ,					
						tblQuy.LaPTChiPhiNganHang ,	
						IIF(tblQuy.LaPTChiPhiNganHang= '0', tblQuy.ChiPhiNganHang, tblQuy.LaPTChiPhiNganHang * tblQuy.ThucThu * tblQuy.ChiPhiNganHang/100) as TongChiPhiNganHang,
						hd.LoaiHoaDon,
    					hd.ID_DoiTuong,
						hd.ID,
    					th.HeSo,
    					tblQuy.NgayLapHoaDon as NgayLapPhieuThu,
    				-- huy PhieuThu --> PTThucThu,HoaHongThucThu = 0		
    					case when TinhChietKhauTheo =1 
    						then case when LoaiHoaDon= 6 then -TienChietKhau else 
    							case when ISNULL(ThucThu,0)= 0 then 0  else TienChietKhau end end end as HoaHongThucThu,
    					case when TinhChietKhauTheo =1 
    						then case when LoaiHoaDon= 6 then PT_ChietKhau else 
    							case when ISNULL(ThucThu,0)= 0 then 0  else PT_ChietKhau end end end as PTThucThu,			    				
    					case when @DateFrom <= hd.NgayLapHoaDon and  hd.NgayLapHoaDon < @DateTo then
    						case when TinhChietKhauTheo = 2 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau end end else 0 end as HoaHongDoanhThu,
    					case when TinhChietKhauTheo =3 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau end end as HoaHongVND,
    					case when @DateFrom <= hd.NgayLapHoaDon and  hd.NgayLapHoaDon < @DateTo then
    						case when TinhChietKhauTheo = 2 then PT_ChietKhau end else 0 end as PTDoanhThu,
    					-- timkiem theo NgayLapHD or NgayLapPhieuThu
    					case when @DateFrom <= hd.NgayLapHoaDon and hd.NgayLapHoaDon < @DateTo then hd.NgayLapHoaDon else tblQuy.NgayLapHoaDon end as NgayLapPhieu ,
    					case when hd.ChoThanhToan='0' then 1 else 2 end as TrangThaiHD
    			
    			from BH_NhanVienThucHien th		
    			join NS_NhanVien nv on th.ID_NhanVien= nv.ID
    			join BH_HoaDon hd on th.ID_HoaDon= hd.ID
    			left join #tblQuyThucTe tblQuy on th.ID_QuyHoaDon = tblQuy.ID	 
    			where th.ID_HoaDon is not null
    				and hd.LoaiHoaDon in (1,19,22,6, 25,3)
    				and hd.ChoThanhToan is not null   
					and exists (select ID from @tblChiNhanh cn where hd.ID_DonVi= cn.ID) --- phai loc ca chi nhanh (Neu khong sẽ bị lấy cả hóa đơn + nhân viên của chi nhánh #)
					and (exists (select LoaiChungTu from @tblChungTu ctu where ctu.LoaiChungTu = hd.LoaiHoaDon))
    				and (exists (select ID from @tblNhanVien nv where th.ID_NhanVien = nv.ID))
    				--chi lay CKDoanhThu hoac CKThucThu/VND exist in Quy_HoaDon or (not exist QuyHoaDon but LoaiHoaDon =6 )
    				and (th.TinhChietKhauTheo != 1 or (th.TinhChietKhauTheo =1 
					and ( exists (select ID from #tempQuy where th.ID_QuyHoaDon = #tempQuy.ID) or  LoaiHoaDon=6)))		
    				and
    				((select count(Name) from @tblSearchString b where     			
    				nv.TenNhanVien like '%'+b.Name+'%'
    				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%'+b.Name+'%'	
    				or hd.MaHoaDon like '%'+b.Name+'%'								
    				)=@count or @count=0)	
    	) tbl
    		left join DM_DoiTuong dt on tbl.ID_DoiTuong= dt.ID
    		where tbl.NgayLapPhieu >= @DateFrom and tbl.NgayLapPhieu < @DateTo and TrangThaiHD = @StatusInvoice

    
    	if @Status_DoanhThu =0
    		insert into @tblDiscountInvoice
    		select *
    		from #temp2
    	else
    		begin
    				if @Status_DoanhThu= 1
    					insert into @tblDiscountInvoice
    					select *
    					from #temp2 where HoaHongDoanhThu > 0 or HoaHongThucThu > 0
    				else
    					if @Status_DoanhThu= 2
    						insert into @tblDiscountInvoice
    						select *
    						from #temp2 where HoaHongDoanhThu > 0 or HoaHongVND > 0
    					else		
    						if @Status_DoanhThu= 3
    							insert into @tblDiscountInvoice
    							select *
    							from #temp2 where HoaHongDoanhThu > 0
    						else	
    							if @Status_DoanhThu= 4
    								insert into @tblDiscountInvoice
    								select *
    								from #temp2 where HoaHongVND > 0 Or HoaHongThucThu > 0
    							else
    								if @Status_DoanhThu= 5
    									insert into @tblDiscountInvoice
    									select *
    									from #temp2 where  HoaHongThucThu > 0
    								else -- 6
    									insert into @tblDiscountInvoice
    									select *
    									from #temp2  where HoaHongVND > 0
    								
    			end;

				declare @tongDoanhThu float, @tongThucThu float

				select @tongDoanhThu = (select sum (tblDT.DoanhThu)
											from
											(
												select  id, MaHoaDon, NgayLapHoaDon, max(DoanhThu) as DoanhThu
												from @tblDiscountInvoice
												group by ID, MaHoaDon, NgayLapHoaDon
											)tblDT
										)
	
				select @tongThucThu = (select sum(tblTT.ThucThu)
										from
										(
											select sum(ThucThu) as ThucThu
											from
											(
											select  id, MaHoaDon, max(ThucThu)  as ThucThu
											from @tblDiscountInvoice
											group by ID, MaHoaDon, NgayLapPhieuThu
											) tbl2 group by ID, MaHoaDon
										)tblTT
										);
    
    	with data_cte
    		as(
    		select * from @tblDiscountInvoice
    		),
    		count_cte
    		as (
    			select count(*) as TotalRow,
    				CEILING(COUNT(*) / CAST(@PageSize as float ))  as TotalPage,
					@tongDoanhThu as TongDoanhThu,
					@tongThucThu as TongThucThu,
    				sum(HoaHongThucThu) as TongHoaHongThucThu,
    				sum(HoaHongDoanhThu) as TongHoaHongDoanhThu,
    				sum(HoaHongVND) as TongHoaHongVND,
    				sum(TongAll) as TongAllAll
    			from data_cte
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.NgayLapHoaDon desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportValueCard_Balance]
    @TextSearch [nvarchar](max),
    @ID_ChiNhanhs [nvarchar](max),
    @DateFrom [nvarchar](max),
    @DateTo [nvarchar](max),
    @Status [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    set nocount on;
	set @DateTo = DATEADD(day,1,@DateTo)
	declare @DateTo_DauKy datetime = dateadd(day,1, @DateFrom)

    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	with data_cte
    	as (
    		select 
    				tblView.ID, tblView.MaDoiTuong, tblView.TenDoiTuong, 
    				ISNULL(tblView.DienThoai,'') as DienThoaiKhachHang,
    				CAST(ISNULL(tblView.SoDuDauKy,0) as float) as SoDuDauKy,
    				CAST(ISNULL(tblView.PhatSinhTang,0) as float) as PhatSinhTang,
    				CAST(ISNULL(tblView.PhatSinhGiam,0) as float) as PhatSinhGiam,
    				ISNULL(tblView.SoDuDauKy,0) + ISNULL(tblView.PhatSinhTang,0)- ISNULL(tblView.PhatSinhGiam,0) as SoDuCuoiKy,
    				case when tblView.TrangThai_TheGiaTri is null or tblView.TrangThai_TheGiaTri = 1 then N'Đang hoạt động'
    				else N'Ngừng hoạt động' end as TrangThai_TheGiaTri,
    				TrangThai
    		from 
    		(
    			select 
    				dt.ID, dt.MaDoiTuong, dt.TenDoiTuong, 
    				dt.TrangThai_TheGiaTri,
    				case when dt.TrangThai_TheGiaTri is null or dt.TrangThai_TheGiaTri = 1 then '11'
    				else '12' end as TrangThai, -- used to where TrangThai_TheGiaTri (1: all, 11: dang hoat dong, 2. Ngung hoat dong)
    				dt.DienThoai,
    				tblTemp.SoDuDauKy,
    				tblTemp.PhatSinhTang,
    				tblTemp.PhatSinhGiam
    			from DM_DoiTuong dt
    			left join 
    			( 
    				select 
    					ID_DoiTuong,
    					SUM(ISNULL(TongThuTheGiaTri,0)) as TongThuTheGiaTri,
    					SUM(ISNULL(SuDungThe,0)) as SuDungThe,
    					SUM(ISNULL(HoanTraTheGiatri,0)) as HoanTraTheGiaTri,
    					SUM(ISNULL(TongThuTheGiaTri,0))  - SUM(ISNULL(SuDungThe,0)) + SUM(ISNULL(HoanTraTheGiatri,0)) as SoDuDauKy,
    					SUM(ISNULL(PhatSinh_ThuTuThe,0)) + SUM(ISNULL(PhatSinh_HoanTraTheGiatri,0)) + SUM(ISNULL(PhatSinhTang_DieuChinhThe,0)) as PhatSinhTang,
    					SUM(ISNULL(PhatSinh_SuDungThe,0)) + SUM(ISNULL(PhatSinhGiam_DieuChinhThe,0)) as PhatSinhGiam
    
    				from (
    					 --- thu the gtri trước thời gian tìm kiếm
    						 select ID as ID_DoiTuong, 
    							SUM(ISNULL(TongThuTheGiaTri,0)) as TongThuTheGiaTri,
    							 null as SuDungThe,
    							 null as HoanTraTheGiatri,						 
    							 null as PhatSinh_ThuTuThe,
    							 null as PhatSinh_SuDungThe,
    							 null as PhatSinh_HoanTraTheGiatri,
    								 null as PhatSinhTang_DieuChinhThe,
    								 null as PhatSinhGiam_DieuChinhThe
    						 from (
    							 SELECT dt.ID, 
    								 case when (hd.LoaiHoaDon=22  or hd.LoaiHoaDon=23) then sum(hd.TongTienHang)
    								 else 0 end as TongThuTheGiaTri
    							 from DM_DoiTuong dt
    							 left join BH_HoaDon hd on hd.ID_DoiTuong = dt.ID
    							 where  hd.NgayLapHoaDon < @DateTo_DauKy 
    							 and hd.ChoThanhToan='0' --and hd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    							 group by dt.ID, hd.LoaiHoaDon
    						 ) tblThuThe group by tblThuThe.ID
    
    					 union all
    					 -- su dung the giatri
    						 select tblSuDungThe.ID_DoiTuong, 
    						  null as TongThuTheGiaTri,
    							 sum(ISNULL(SuDungThe,0)) as SuDungThe,
    							 null as HoanTraTheGiatri,						
    							 null as PhatSinh_ThuTuThe,
    							 null as PhatSinh_SuDungThe,
    							 null as PhatSinh_HoanTraTheGiatri,
    								 null as PhatSinhTang_DieuChinhThe,
    								 null as PhatSinhGiam_DieuChinhThe
    			
    						 from (
    							 SELECT qct.ID_DoiTuong,
    								case when qhd.LoaiHoaDon= 12 then 0 else sum(iif(qct.HinhThucThanhToan=4,qct.TienThu,0)) end as SuDungThe
    							 from Quy_HoaDon_ChiTiet qct
    							 left join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
    							 left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    							 where qhd.NgayLapHoaDon < @DateTo_DauKy
    							 --and qhd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    							 and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    							 and hd.ChoThanhToan ='0'
    							 group by qct.ID_DoiTuong, qhd.LoaiHoaDon,hd.ChoThanhToan
    						 ) tblSuDungThe group by tblSuDungThe.ID_DoiTuong
    
    				 union all
    					  -- hoan tra tien the giatri
    						select ID_DoiTuong, 
    							null as TongThuTheGiaTri,
    							null as SuDungThe,
    							SUM(ISNULL(HoanTraTheGiatri,0)) as HoanTraTheGiatri,						
    							null as PhatSinh_ThuTuThe,
    							null as PhatSinh_SuDungThe,
    							null as PhatSinh_HoanTraTheGiatri,
    								null as PhatSinhTang_DieuChinhThe,
    								null as PhatSinhGiam_DieuChinhThe
    						from (
    								SELECT qct.ID_DoiTuong,
    								case when qhd.LoaiHoaDon= 11 then 0 else sum(iif(qct.HinhThucThanhToan=4,qct.TienThu,0)) end as HoanTraTheGiatri
    								from Quy_HoaDon_ChiTiet qct
    								left join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
    								left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    								where  qhd.NgayLapHoaDon < @DateTo_DauKy
    								--and qhd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    								and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    								and hd.ChoThanhToan ='0'
    								group by qct.ID_DoiTuong, qhd.LoaiHoaDon,hd.ChoThanhToan
    							) tblSuDungThe group by tblSuDungThe.ID_DoiTuong 
    
    				 union all
    					   --- thu the gtri tại thời điểm hiện tại
    						 select ID_DoiTuong, 
    					 		 null as TongThuTheGiaTri,
    							 null as SuDungThe,
    							 null as HoanTraTheGiatri,
    							 SUM(ISNULL(TongThuTheGiaTri,0)) as PhatSinh_ThuTuThe,
    							 null as PhatSinh_SuDungThe,
    							 null as PhatSinh_HoanTraTheGiatri,
    								 null as PhatSinhTang_DieuChinhThe,
    								 null as PhatSinhGiam_DieuChinhThe
    						 from (
    							 SELECT hd.ID_DoiTuong, 
    								 case when (hd.LoaiHoaDon=22) then sum(hd.TongTienHang)
    								 else 0 end as TongThuTheGiaTri
    							 from BH_HoaDon hd 
    							 where hd.NgayLapHoaDon between @DateFrom and @DateTo
    							 and hd.ChoThanhToan='0' --and hd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    							 group by hd.ID_DoiTuong, hd.LoaiHoaDon
    						 ) tblThuThe2 group by tblThuThe2.ID_DoiTuong
    
    				union all
    					 -- su dung the giatri tại thời điểm hiện tại
    						 select tblSuDungThe2.ID_DoiTuong, 
    							 null as TongThuTheGiaTri,
    							 null as SuDungThe,
    							 null as HoanTraTheGiatri,						 
    							 null as PhatSinh_ThuTuThe,
    							 sum(ISNULL(SuDungThe,0)) as PhatSinh_SuDungThe,
    							 null as PhatSinh_HoanTraTheGiatri,
    								 null as PhatSinhTang_DieuChinhThe,
    								 null as PhatSinhGiam_DieuChinhThe
    			
    						 from (
    							 SELECT qct.ID_DoiTuong,
    								case when qhd.LoaiHoaDon= 12 then 0 else sum(iif(qct.HinhThucThanhToan=4,qct.TienThu,0)) end as SuDungThe
    							 from Quy_HoaDon_ChiTiet qct
    							 left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    							 left join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
    							 where qhd.NgayLapHoaDon between @DateFrom and @DateTo
    							 --and qhd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    							 and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    							 and hd.ChoThanhToan ='0'
    							 group by qct.ID_DoiTuong, qhd.LoaiHoaDon,hd.ChoThanhToan
    						 ) tblSuDungThe2 group by tblSuDungThe2.ID_DoiTuong
    
    					 union all
    					 -- phat sinh tang do điều chỉnh
    						select ID_DoiTuong, 
    					 		 null as TongThuTheGiaTri,
    							 null as SuDungThe,
    							 null as HoanTraTheGiatri,
    							 null as PhatSinh_ThuTuThe,
    							 null as PhatSinh_SuDungThe,
    							 null as PhatSinh_HoanTraTheGiatri,
    							 SUM(ISNULL(TongThuTheGiaTri,0)) as PhatSinhTang_DieuChinhThe,
    								 null as PhatSinhTang_DieuChinhThe
    
    						 from (
    							 SELECT hd.ID_DoiTuong, 
    								 case when (hd.LoaiHoaDon=23) then sum(hd.TongTienHang)
    								 else 0 end as TongThuTheGiaTri
    							 from BH_HoaDon hd 
    							 where  hd.NgayLapHoaDon between @DateFrom and @DateTo
    							 and hd.ChoThanhToan='0' --and hd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    								 and ISNULL(hd.TongTienHang,0) > 0
    							 group by hd.ID_DoiTuong, hd.LoaiHoaDon
    						 ) tblThuThe2 group by tblThuThe2.ID_DoiTuong
    
    					 union all
    					 -- phat sinh giam do điều chỉnh
    						select ID_DoiTuong, 
    					 		 null as TongThuTheGiaTri,
    							 null as SuDungThe,
    							 null as HoanTraTheGiatri,
    							 null as PhatSinh_ThuTuThe,
    							 null as PhatSinh_SuDungThe,
    							 null as PhatSinh_HoanTraTheGiatri,
    								 null as PhatSinhTang_DieuChinhThe,
    							 SUM(ISNULL(TongThuTheGiaTri,0)* -1) as PhatSinhGiam_DieuChinhThe
    
    						 from (
    							 SELECT hd.ID_DoiTuong, 
    								 case when (hd.LoaiHoaDon=23) then sum(hd.TongTienHang)
    								 else 0 end as TongThuTheGiaTri
    							 from BH_HoaDon hd 
    							 where hd.NgayLapHoaDon between @DateFrom and @DateTo
    							 and hd.ChoThanhToan='0' --and hd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    								 and ISNULL(hd.TongTienHang,0) < 0
    							 group by hd.ID_DoiTuong, hd.LoaiHoaDon
    						 ) tblThuThe2 group by tblThuThe2.ID_DoiTuong
    
    				union all
    					  -- hoan tra tien the giatri tại thời điểm hiện tại
    						select ID_DoiTuong, 
    							null as TongThuTheGiaTri,
    							null as SuDungThe,
    							null as HoanTraTheGiatri,						
    							null as PhatSinh_ThuTuThe,
    							null as PhatSinh_SuDungThe,
    							SUM(ISNULL(HoanTraTheGiatri,0)) as PhatSinh_HoanTraTheGiatri,
    								null as PhatSinhTang_DieuChinhThe,
    								null as PhatSinhGiam_DieuChinhThe
    						from (
    								SELECT qct.ID_DoiTuong,
    								case when qhd.LoaiHoaDon= 11 then 0 else sum(iif(qct.HinhThucThanhToan=4,qct.TienThu,0)) end as HoanTraTheGiatri
    								from Quy_HoaDon_ChiTiet qct
    								left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    								left join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
    								where  qhd.NgayLapHoaDon between @DateFrom and @DateTo
    								--and qhd.ID_DonVi in (select * from dbo.splitstring (@ID_ChiNhanhs))
    								and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    								and hd.ChoThanhToan ='0'
    								group by qct.ID_DoiTuong, qhd.LoaiHoaDon,hd.ChoThanhToan
    							) tblSuDungThe2 group by tblSuDungThe2.ID_DoiTuong 
    
    					) tblDoiTuong_The group by tblDoiTuong_The.ID_DoiTuong
    					
    			) tblTemp on dt.ID= tblTemp.ID_DoiTuong
    			where (dt.TheoDoi is null or dt.TheoDoi = 0) and dt.LoaiDoiTuong =1
    				and
    					 
    							((select count(Name) from @tblSearchString b where    
								dt.DienThoai like '%'+b.Name+'%'
    							or dt.MaDoiTuong like '%'+b.Name+'%'
    							or dt.TenDoiTuong like '%'+b.Name+'%'
    							or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    							or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'				
    							)=@count or @count=0)	
    
    		) tblView 
    		where tblView.TrangThai like @Status
    		and ISNULL(tblView.SoDuDauKy,0) + ISNULL(tblView.PhatSinhTang,0)- ISNULL(tblView.PhatSinhGiam,0) > 0
    	),
    	count_cte
    	as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
    				sum(SoDuDauKy) as TongSoDuDauKy,
    				sum(PhatSinhTang) as TongPhatSinhTang,
    				sum(PhatSinhGiam) as TongPhatSinhGiam,
    				sum(SoDuCuoiKy) as TongSoDuCuoiKy
    			from data_cte
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.MaDoiTuong
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[SP_GetHoaDonAndSoQuy_FromIDDoiTuong]
    @ID_DoiTuong [nvarchar](max),
    @ID_DonVi [nvarchar](max)
AS
BEGIN
SET NOCOUNT ON;

declare @tblChiNhanh table (ID uniqueidentifier)
insert into @tblChiNhanh
select name from dbo.splitstring(@ID_DonVi)


	DECLARE @tblHoaDon TABLE(ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, LoaiHoaDon INT, GiaTri FLOAT);
	DECLARE @LoaiDoiTuong INT;
	SELECT @LoaiDoiTuong = LoaiDoiTuong FROM DM_DoiTuong WHERE ID = @ID_DoiTuong;
	IF(@LoaiDoiTuong = 3)
	BEGIN
		INSERT INTO @tblHoaDon
    	select hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
			hd.PhaiThanhToanBaoHiem as GiaTri
    	from BH_HoaDon hd
    	where hd.ID_BaoHiem like @ID_DoiTuong and hd.ID_DonVi in (select ID from @tblChiNhanh)
    	and hd.LoaiHoaDon not in (3,23) -- dieu chinh the: khong lien quan cong no
		and hd.ChoThanhToan ='0'
	END
	ELSE
	BEGIN
		INSERT INTO @tblHoaDon
		select *
		from
		(
		select hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
			case hd.LoaiHoaDon
				when 4 then ISNULL(hd.TongThanhToan,0)
				when 6 then - ISNULL(hd.TongThanhToan,0)
				when 7 then - ISNULL(hd.TongThanhToan,0)
			else
    			ISNULL(hd.PhaiThanhToan,0)
    		end as GiaTri
    	from BH_HoaDon hd
		join @tblChiNhanh cn on hd.ID_DonVi= cn.ID
    	where hd.ID_DoiTuong like @ID_DoiTuong 
    	and hd.LoaiHoaDon not in (3,23,31) 
		and hd.ChoThanhToan ='0'

		union all
		---- chiphi dichvu
		select 
			cp.ID_HoaDon, hd.MaHoaDon, hd.NgayLapHoaDon, 125 as LoaiHoaDon,
			sum(cp.ThanhTien) as GiaTri			
		from BH_HoaDon_ChiPhi cp
		join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
		join @tblChiNhanh cn on hd.ID_DonVi= cn.ID
		where hd.ChoThanhToan= 0
		and cp.ID_NhaCungCap= @ID_DoiTuong
		group by cp.ID_HoaDon, hd.MaHoaDon, hd.NgayLapHoaDon,hd.LoaiHoaDon
		)a
	END

	---select * from @tblHoaDon
		
		SELECT *, 0 as LoaiThanhToan
		FROM @tblHoaDon
    	union
    	-- get list Quy_HD (không lấy Quy_HoaDon thu từ datcoc)
		select * from
		(
    		select qhd.ID, qhd.MaHoaDon, qhd.NgayLapHoaDon, qhd.LoaiHoaDon ,
			case when dt.LoaiDoiTuong = 1 OR dt.LoaiDoiTuong = 3 then
				case when qhd.LoaiHoaDon= 11 then -sum(qct.TienThu) else iif (max(LoaiThanhToan)=4, -sum(qct.TienThu),sum(qct.TienThu)) end
			else 
    			case when qhd.LoaiHoaDon = 11 then sum(qct.TienThu) else -sum(qct.TienThu) end
    		end as GiaTri,
			iif(qhd.PhieuDieuChinhCongNo='1',2, max(qct.LoaiThanhToan)) as LoaiThanhToan --- 1.coc, 2.dieuchinhcongno, 3.khong butru congno, 4.tralai sodu TGT			
    		from Quy_HoaDon qhd	
    		join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
    		join DM_DoiTuong dt on qct.ID_DoiTuong= dt.ID
			left join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID		
    		where qct.ID_DoiTuong like @ID_DoiTuong
			and exists (select ID from @tblChiNhanh cn where qhd.ID_DonVi= cn.ID) ---- chi lay phieuthu/chi cua chi nhanh dang chon
			and qct.HinhThucThanhToan !=6	
			and qct.LoaiThanhToan !=4
			and (qct.LoaiThanhToan is null or qct.LoaiThanhToan !=3) ---- khong get phieuthu/chi khong lienquan congno
			and (qhd.TrangThai is null or qhd.TrangThai='1') -- van phai lay phieu thu tu the --> trừ cong no KH
			group by qhd.ID, qhd.MaHoaDon, qhd.NgayLapHoaDon, qhd.LoaiHoaDon,dt.LoaiDoiTuong,qhd.PhieuDieuChinhCongNo
		) a where a.GiaTri != 0 -- khong lay phieudieuchinh diem

	
END
");

			Sql(@"ALTER PROCEDURE [dbo].[TongQuanDoanhThuCongNo]
    @IdChiNhanhs [nvarchar](max),
    @DateFrom [datetime],
    @DateTo [datetime]
AS
BEGIN
    SET NOCOUNT ON;


    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    -- Insert statements for procedure here
    	DECLARE @tblHoaDon TABLE(ID UNIQUEIDENTIFIER, LoaiHoaDon INT, TongThanhToan FLOAT, PhaiThanhToan FLOAT, TongTienThue FLOAT, IDHoaDon UNIQUEIDENTIFIER);
    	INSERT INTO @tblHoaDon
    	SELECT ID, LoaiHoaDon, TongThanhToan, PhaiThanhToan, TongTienThue + TongTienThueBaoHiem, hd.ID_HoaDon FROM BH_HoaDon hd
    	INNER JOIN @tblDonVi dv ON hd.ID_DonVi = dv.ID_DonVi
    	WHERE hd.LoaiHoaDon IN (1, 4, 6, 7, 19, 25)
    	AND hd.ChoThanhToan = 0
    	AND hd.NgayLapHoaDon BETWEEN @DateFrom AND @DateTo;

		--SELECT * FROM @tblHoaDon
    
    	DECLARE @tblSoQuy TABLE(ID UNIQUEIDENTIFIER, LoaiHoaDon INT, TienMat FLOAT, TienGui FLOAT, TienThu FLOAT, IDHoaDonLienQuan UNIQUEIDENTIFIER);
    	INSERT INTO @tblSoQuy
    	SELECT qhd.ID, qhd.LoaiHoaDon, qhdct.TienMat, qhdct.TienGui, qhdct.TienThu, qhdct.ID_HoaDonLienQuan FROM Quy_HoaDon qhd
    	INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    	INNER JOIN @tblDonVi dv ON qhd.ID_DonVi = dv.ID_DonVi
    	WHERE qhd.NgayLapHoaDon BETWEEN @DateFrom AND @DateTo
		and (qhd.TrangThai = 1 or qhd.TrangThai is null)
		and (qhd.PhieuDieuChinhCongNo != 1 or qhd.PhieuDieuChinhCongNo is null)
    	AND qhd.LoaiHoaDon IN (11, 12);
		--SELECT * FROM @tblSoQuy
    
    	DECLARE @DoanhThuSuaChua FLOAT,
    	@DoanhThuBanHang FLOAT,
    	@PhaiTraKhachHang FLOAT,
    	@PhaiTraNhaCungCap FLOAT,
    	@PhaiThuNhaCungCap FLOAT,
    	@Thu_TienMat FLOAT,
    	@Thu_NganHang FLOAT,
    	@Chi_TienMat FLOAT,
    	@Chi_NganHang FLOAT,
    	@HoaDonDaThu FLOAT,
    	@HoaDonDaChi FLOAT,
		@ThueSuaChua FLOAT,
		@ThueBanHang FLOAT;
    	SELECT @DoanhThuSuaChua =SUM(CASE WHEN hd.LoaiHoaDon = 25 THEN hd.TongThanhToan ELSE 0 END), 
    		@PhaiTraKhachHang = SUM(CASE WHEN hd.LoaiHoaDon = 6 THEN hd.TongThanhToan ELSE 0 END),
    		@DoanhThuBanHang = SUM(CASE WHEN hd.LoaiHoaDon IN (1, 19)
    		THEN
    			hd.PhaiThanhToan
    			ELSE 0
    		END),
    	@PhaiTraNhaCungCap = SUM(CASE WHEN hd.LoaiHoaDon = 4 THEN hd.PhaiThanhToan ELSE 0 END),
    	@PhaiThuNhaCungCap = SUM(CASE WHEN hd.LoaiHoaDon = 7 THEN hd.PhaiThanhToan ELSE 0 END),
		@ThueSuaChua = SUM(CASE WHEN hd.LoaiHoaDon = 25 THEN hd.TongTienThue ELSE 0 END),
		@ThueBanHang = SUM(CASE WHEN hd.LoaiHoaDon IN (1, 19) THEN hd.TongTienThue ELSE 0 END)
    	FROM @tblHoaDon hd;
    
    	SELECT @Thu_TienMat = SUM(CASE WHEN sq.LoaiHoaDon = 11 THEN sq.TienMat ELSE 0 END), 
    	@Thu_NganHang = SUM(CASE WHEN sq.LoaiHoaDon = 11 THEN sq.TienGui ELSE 0 END),
    	@Chi_TienMat = SUM(CASE WHEN sq.LoaiHoaDon = 12 THEN sq.TienMat ELSE 0 END),
    	@Chi_NganHang = SUM(CASE WHEN sq.LoaiHoaDon = 12 THEN sq.TienGui ELSE 0 END)
    	FROM @tblSoQuy sq;
    
    	SELECT @HoaDonDaThu = SUM(CASE WHEN sq.LoaiHoaDon = 11 THEN sq.ThanhToan ELSE 0 END),  
    	@HoaDonDaChi = SUM(CASE WHEN sq.LoaiHoaDon = 12 AND hd.LoaiHoaDon != 7 THEN sq.ThanhToan ELSE 0 END) FROM @tblHoaDon hd
    	INNER JOIN (SELECT IDHoaDonLienQuan, SUM(TienThu) AS ThanhToan, LoaiHoaDon FROM @tblSoQuy GROUP BY IDHoaDonLienQuan, LoaiHoaDon) sq
    	ON hd.ID = sq.IDHoaDonLienQuan OR (hd.IDHoaDon = sq.IDHoaDonLienQuan AND hd.LoaiHoaDon != 6)
    
    	SET @DoanhThuSuaChua = ISNULL(@DoanhThuSuaChua, 0);
    	SET @DoanhThuBanHang = ISNULL(@DoanhThuBanHang, 0);
    	SET @PhaiTraKhachHang = ISNULL(@PhaiTraKhachHang, 0);
    	SET @PhaiTraNhaCungCap = ISNULL(@PhaiTraNhaCungCap, 0);
    	SET @PhaiThuNhaCungCap = ISNULL(@PhaiThuNhaCungCap, 0);
    	SET @Thu_TienMat = ISNULL(@Thu_TienMat, 0);
    	SET @Thu_NganHang = ISNULL(@Thu_NganHang, 0);
    	SET @Chi_TienMat = ISNULL(@Chi_TienMat, 0);
    	SET @Chi_NganHang = ISNULL(@Chi_NganHang, 0);
    	SET @HoaDonDaThu = ISNULL(@HoaDonDaThu, 0);
    	SET @HoaDonDaChi = ISNULL(@HoaDonDaChi, 0);
		SET @ThueBanHang = ISNULL(@ThueBanHang, 0);
		SET @ThueSuaChua = ISNULL(@ThueSuaChua, 0)
    
    	SELECT @DoanhThuSuaChua - @ThueSuaChua AS DoanhThuSuaChua, @DoanhThuBanHang - @ThueBanHang AS DoanhThuBanHang,
    	@DoanhThuBanHang + @DoanhThuSuaChua - @ThueBanHang - @ThueSuaChua AS TongDoanhThu, @DoanhThuBanHang + @DoanhThuSuaChua + @PhaiThuNhaCungCap - @HoaDonDaThu AS CongNoPhaiThu,
    	@PhaiTraNhaCungCap + @PhaiTraKhachHang - @HoaDonDaChi AS CongNoPhaiTra, 
    	-(@DoanhThuBanHang + @DoanhThuSuaChua + @PhaiThuNhaCungCap - @HoaDonDaThu) + (@PhaiTraNhaCungCap + @PhaiTraKhachHang - @HoaDonDaChi) AS TongCongNo,
    	@Thu_TienMat AS ThuTienMat, @Thu_NganHang AS ThuNganHang, @Thu_TienMat + @Thu_NganHang AS TongTienThu,
    	@Chi_TienMat AS ChiTienMat, @Chi_NganHang AS ChiNganHang, @Chi_TienMat + @Chi_NganHang AS TongTienChi;
END");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateStatusCongBoSung_WhenCreatBangLuong]
    @ID_BangLuong [uniqueidentifier],
    @FromDate [datetime],
    @ToDate [datetime]
AS
BEGIN
    SET NOCOUNT ON;	
		set @ToDate = DATEADD(MILLISECOND, 1, @ToDate) ---- todate = denngay in NS_BangLuong

		---- get infor bangluong ----
    	declare @statusSalary int, @ID_DonVi uniqueidentifier
			select @statusSalary = TrangThai, @ID_DonVi = ID_DonVi from NS_BangLuong where ID= @ID_BangLuong
    	declare @statusCong int = @statusSalary

		if @statusSalary = 1 set @statusCong = 2

		--- bangluong - 0 (huy) --> cong = 1 (reset ve tao moi)
		--- bangluong - 1 (tamluu) --> cong = 2
		--- bangluong - 2 (cần tính lại) --> cong = 2
		--- bangluong - 3 (da chot) = cong
		--- bangluong - 4 (da thanh toan) = cong
	
    	if	@statusSalary like '[1-3]' -- @statusSalary= 1/2/3
			begin    						
				update bs set TrangThai = @statusCong, 
    						 ID_BangLuongChiTiet = blct.ID
    			from NS_CongBoSung bs
    			join NS_BangLuong_ChiTiet blct on bs.ID_NhanVien= blct.ID_NhanVien
    			where blct.ID_BangLuong= @ID_BangLuong
    			and bs.NgayCham between @FromDate and  @ToDate
				and bs.TrangThai in (1,2) --- taomoi, tamluu
				and bs.ID_DonVi = @ID_DonVi
			end
    	else
			begin
			if @statusSalary = 4
				begin
					----- get list id_bangluongchitiet
					select ID into #tblCTLuong from NS_BangLuong_ChiTiet where ID_BangLuong= @ID_BangLuong
	
					update bs set TrangThai = iif(soquy.DaThanhToan > 0 ,4,3)    					
    				from NS_CongBoSung bs
    				join #tblCTLuong blct on bs.ID_BangLuongChiTiet = blct.ID 						
					join (
						---- get soquy ttluong
						select qct.ID_BangLuongChiTiet,
							sum(iif(qhd.TrangThai ='0',0, qct.TienThu + isnull(qct.TruTamUngLuong,0))) as DaThanhToan
						from Quy_HoaDon_ChiTiet qct 
						join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
						where exists (select ID from #tblCTLuong ctluong where qct.ID_BangLuongChiTiet = ctluong.ID)
						group by qct.ID_BangLuongChiTiet
					) soquy on blct.ID= soquy.ID_BangLuongChiTiet
    				where bs.NgayCham between @FromDate and  @ToDate
				end
				else
					 ---- huybangluong
					begin													   		
							update bs set TrangThai = 1, 
    								ID_BangLuongChiTiet = null
    						from NS_CongBoSung bs
    						join NS_BangLuong_ChiTiet blct on bs.ID_BangLuongChiTiet = blct.ID
    						where blct.ID_BangLuong= @ID_BangLuong
    						and bs.NgayCham between @FromDate and  @ToDate									
					end    
			end   
END

");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateTonLuyKeCTHD_whenUpdate]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanhInput [uniqueidentifier],
    @NgayLapHDOld [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    
    		DECLARE @NgayLapHDNew DATETIME;   
    		DECLARE @NgayNhanHang DATETIME;
    		declare @tblHoaDonChiTiet ChiTietHoaDonEdit -- table user defined
    		DECLARE @IDCheckIn  UNIQUEIDENTIFIER, @YeuCau NVARCHAR(MAX),  @LoaiHoaDon INT, @NgayLapHDMin DATETIME;
    		DECLARE @tblChiTiet TABLE (ID_HangHoa UNIQUEIDENTIFIER not null, ID_LoHang UNIQUEIDENTIFIER null, ID_DonViQuiDoi UNIQUEIDENTIFIER not null, TyLeChuyenDoi float not null)
    		DECLARE @LuyKeDauKy TABLE(ID_LoHang UNIQUEIDENTIFIER, ID_HangHoa UNIQUEIDENTIFIER, TonLuyKe FLOAT);
    		DECLARE @hdctUpdate TABLE(ID UNIQUEIDENTIFIER, ID_DonVi UNIQUEIDENTIFIER, ID_CheckIn UNIQUEIDENTIFIER, TonLuyKe FLOAT, LoaiHoaDon INT, 
    		MaHoaDon nvarchar(max), NgayLapHoaDon datetime, YeuCau nvarchar(max));
    
    		--  get NgayLapHD by IDHoaDon: if update HDNhanHang (loai 10, yeucau = 4 --> get NgaySua
    		select 
    			@NgayLapHDNew = NgayLapHoaDon,
    			@NgayNhanHang = NgaySua,
    			@LoaiHoaDon = LoaiHoaDon, @YeuCau = YeuCau, @IDCheckIn = ID_CheckIn
				--@IDChiNhanhInput = ID_DonVi
    		from (
    					select LoaiHoaDon, YeuCau, ID_CheckIn, ID_DonVi, NgaySua, 
						case when @IDChiNhanhInput = ID_CheckIn and YeuCau !='1' then NgaySua else NgayLapHoaDon end as NgayLapHoaDon
    					from BH_HoaDon where ID = @IDHoaDonInput) a
    
    		-- alway get Ngay min --> compare to update TonLuyKe
    		IF(@NgayLapHDOld > @NgayLapHDNew)
    			SET @NgayLapHDMin = @NgayLapHDNew;
    		ELSE
    			SET @NgayLapHDMin = @NgayLapHDOld;
    
    		-- get cthd update by IDHoaDon
    		INSERT INTO @tblChiTiet
    		SELECT 
    			qd.ID_HangHoa, ct.ID_LoHang, ct.ID_DonViQuiDoi, qd.TyLeChuyenDoi
    		FROM BH_HoaDon_ChiTiet ct
    		INNER JOIN BH_HoaDon hd ON hd.ID = ct.ID_HoaDon			
    		INNER JOIN DonViQuiDoi qd ON qd.ID = ct.ID_DonViQuiDoi			
    		INNER JOIN DM_HangHoa hh on hh.ID = qd.ID_HangHoa    		
    		WHERE hd.ID = @IDHoaDonInput AND hh.LaHangHoa = 1 
    		GROUP BY qd.ID_HangHoa,ct.ID_DonViQuiDoi,qd.TyLeChuyenDoi, ct.ID_LoHang, hd.ID_DonVi, hd.ID_CheckIn, hd.YeuCau, hd.NgaySua, hd.NgayLapHoaDon;	
    		insert into @tblHoaDonChiTiet select * from @tblChiTiet			
    				
    		-- get cthd has KiemKe group by ID_HangHoa, ID_LoHang
    		declare @tblHangKiemKe table (NgayKiemKe datetime, ID_HangHoa uniqueidentifier null, ID_LoHang uniqueidentifier null)
    		insert into @tblHangKiemKe
    		select distinct NgayLapHoaDon, qd.ID_HangHoa, ct.ID_LoHang
    			from BH_HoaDon_ChiTiet ct 
    			join BH_HoaDon hd ON hd.ID = ct.ID_HoaDon		
    			join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    			join @tblChiTiet tblct ON qd.ID_HangHoa = tblct.ID_HangHoa AND (ct.ID_LoHang = tblct.ID_LoHang OR ct.ID_LoHang IS NULL)	
    			WHERE hd.ChoThanhToan = 0
    			and hd.LoaiHoaDon= 9
    			and hd.ID_DonVi = @IDChiNhanhInput and NgayLapHoaDon >= @NgayLapHDMin
    			group by qd.ID_HangHoa, ct.ID_LoHang, hd.NgayLapHoaDon				
    		
    		-- get cthd liên quan
    		select
    			ct.ID, 
    			ct.ID_LoHang,
				-- chatlieu = 5 (cthd bi xoa khi updateHD), chatlieu =2 (tra gdv  - khong cong lai tonkho)
    			case when ct.ChatLieu= '5' or ct.ChatLieu ='2' then 0 else SoLuong end as SoLuong, 
    			case when ct.ChatLieu= '5' then 0 else TienChietKhau end as TienChietKhau,
    			case when ct.ChatLieu= '5' then 0 else ct.ThanhTien end as ThanhTien,-- kiemke bi huy
    			case when hd.LoaiHoaDon= 10 and  hd.YeuCau = '4' AND hd.ID_CheckIn = @IDChiNhanhInput then ct.TonLuyKe_NhanChuyenHang else ct.TonLuyKe end as TonDauKy,
    			qd.ID_HangHoa,
    			qd.TyLeChuyenDoi,
    			hd.MaHoaDon,
    			hd.LoaiHoaDon,
    			hd.ID_DonVi,
    			hd.ID_CheckIn,
    			hd.YeuCau,								
    			case when hd.YeuCau = '4' AND hd.ID_CheckIn = @IDChiNhanhInput then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon
    		into #temp
    		from BH_HoaDon_ChiTiet ct
    		join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
    		join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    		join @tblChiTiet ctupdate on qd.ID_HangHoa = ctupdate.ID_HangHoa AND (ct.ID_LoHang = ctupdate.ID_LoHang OR ct.ID_LoHang IS NULL)	
    		WHERE (hd.ChoThanhToan = 0 or (hd.LoaiHoaDon= 3 and hd.ChoThanhToan= '1'))	-- used to tao BG (chuaduyet), sau do clcik Duyet
    		AND (hd.ID_DonVi = @IDChiNhanhInput OR (hd.ID_CheckIn = @IDChiNhanhInput AND hd.YeuCau = '4'))
    
    		-- table cthd has ID_HangHoa exist cthd kiemke
    		declare @cthdHasKiemKe table (ID_HangHoa uniqueidentifier, ID_LoHang uniqueidentifier)
    		declare @tblNgayKiemKe table (NgayKiemKe datetime)
    
    		declare @count float= (select count(*) from @tblHangKiemKe)
    		--if @count > 0
    			begin						
    				declare @ID_HangHoa uniqueidentifier, @ID_LoHang uniqueidentifier				
    				DECLARE Cur_tblKiemKe CURSOR SCROLL LOCAL FOR
    				select ID_HangHoa, ID_LoHang
    				from @tblHangKiemKe
    				order by NgayKiemKe
    
    				OPEN Cur_tblKiemKe -- cur 1
    			FETCH FIRST FROM Cur_tblKiemKe
    				INTO @ID_HangHoa, @ID_LoHang
    				WHILE @@FETCH_STATUS = 0
    				BEGIN	
    						if not exists (select * from @cthdHasKiemKe kk where kk.ID_HangHoa= @ID_HangHoa and (kk.ID_LoHang= @ID_LoHang OR kk.ID_LoHang is null))
    							begin
    								-- get list NgayKiemKe by ID_HangHoa & ID_LoHang								
    								declare @NgayKiemKe datetime
    								declare @FromDate datetime = @NgayLapHDMin
    
    								-- get cac khoang thoigian kiemke
    								insert into @tblNgayKiemKe
    								select *
    								from
    									( select NgayKiemKe 
    									from @tblHangKiemKe kk where kk.ID_HangHoa = @ID_HangHoa and (kk.ID_LoHang= @ID_LoHang or kk.ID_LoHang is null)						
    									union 
    										select GETDATE() as NgayKiemKe
    									) b order by NgayKiemKe
    
    								DECLARE Cur_NgayKiemKe CURSOR SCROLL LOCAL FOR								
    								select NgayKiemKe from @tblNgayKiemKe
    
    								OPEN Cur_NgayKiemKe -- cur 2
    							FETCH FIRST FROM Cur_NgayKiemKe
    								INTO @NgayKiemKe
    								WHILE @@FETCH_STATUS = 0
    									begin											
    										insert into @cthdHasKiemKe values(@ID_HangHoa, @ID_LoHang)
    										-- get tondauky 
    										if @FromDate = @NgayLapHDMin and @LoaiHoaDon !=9		
    											begin
    												insert into @LuyKeDauKy
    												select 
    													ID_LoHang,ID_HangHoa,TonDauKy																		
    												from
    													(
    													select 
    														ID_LoHang,ID_HangHoa,TonDauKy,										
    														ROW_NUMBER() OVER (PARTITION BY ID_HangHoa, ID_LoHang ORDER BY NgayLapHoaDon DESC) AS RN 
    													from #temp
    													where NgayLapHoaDon < @FromDate		
    													and #temp.ID_HangHoa = @ID_HangHoa AND (#temp.ID_LoHang = @ID_LoHang OR #temp.ID_LoHang IS NULL)										
    													) luyke	
    												where luyke.RN= 1									
    											end
    										else
    											begin
    												insert into @LuyKeDauKy
    												select 
    													ID_LoHang,ID_HangHoa,TonDauKy
    												from
    													(
    													select 
    														ID_LoHang,ID_HangHoa,TonDauKy,
    														ROW_NUMBER() OVER (PARTITION BY ID_HangHoa, ID_LoHang ORDER BY NgayLapHoaDon DESC) AS RN 
    													from #temp
    													where NgayLapHoaDon <=  @FromDate 
    													and #temp.ID_HangHoa = @ID_HangHoa AND (#temp.ID_LoHang = @ID_LoHang OR #temp.ID_LoHang IS NULL)		
    													) luyke	
    												where luyke.RN= 1
    											end
    		
    										--- tinh lai tonluyke
    										INSERT INTO @hdctUpdate
    										select ID, ID_DonVi, ID_CheckIn,
    												ISNULL(lkdk.TonLuyKe, 0) + 
    												(SUM(IIF(LoaiHoaDon IN (1, 5, 7, 8), -1 * a.SoLuong* a.TyLeChuyenDoi, 
    											IIF(LoaiHoaDon IN (4, 6, 18,13,14), SoLuong * TyLeChuyenDoi, 				
    												IIF((LoaiHoaDon = 10 AND YeuCau = '1') OR (ID_CheckIn IS NOT NULL AND ID_CheckIn != @IDChiNhanhInput AND LoaiHoaDon = 10 AND YeuCau = '4') AND ID_DonVi = @IDChiNhanhInput, -1 * TienChietKhau* TyLeChuyenDoi, 				
    											IIF(a.LoaiHoaDon = 10 AND a.YeuCau = '4' AND a.ID_CheckIn = @IDChiNhanhInput, a.TienChietKhau* a.TyLeChuyenDoi, 0))))) 
    												OVER(PARTITION BY a.ID_HangHoa, a.ID_LoHang ORDER BY NgayLapHoaDon)) AS TonLuyKe,
    												LoaiHoaDon, MaHoaDon,NgayLapHoaDon, YeuCau
    										from
    											(							
    											select distinct
    												ID,
    												ID_LoHang,
    												SoLuong,
    												TienChietKhau,
    												ThanhTien,
    												ID_HangHoa,
    												TyLeChuyenDoi,
    												MaHoaDon,
    												LoaiHoaDon,
    												NgayLapHoaDon,
    												ID_DonVi,
    												ID_CheckIn,
    												YeuCau
    											from #temp
    											where NgayLapHoaDon >= @FromDate
    												and NgayLapHoaDon < @NgayKiemKe
    												and #temp.ID_HangHoa = @ID_HangHoa AND (#temp.ID_LoHang = @ID_LoHang or #temp.ID_LoHang IS NULL	)						
    											) a
    										LEFT JOIN @LuyKeDauKy lkdk ON lkdk.ID_HangHoa = a.ID_HangHoa AND (lkdk.ID_LoHang = a.ID_LoHang OR a.ID_LoHang IS NULL)	
    						
    										-- xóa TonLuyKe trước đó để lấy TonLuyKe mới theo khoảng thời gian		
    										set @FromDate = @NgayKiemKe
    										--select *, 1 as after1 from @LuyKeDauKy
    										delete from @LuyKeDauKy															
    										FETCH NEXT FROM Cur_NgayKiemKe INTO @NgayKiemKe
    									end
    								CLOSE Cur_NgayKiemKe  
    								DEALLOCATE Cur_NgayKiemKe 
    							end		
    
    						-- delete & assign again in for loop
    						delete from @tblNgayKiemKe
    						FETCH NEXT FROM Cur_tblKiemKe INTO @ID_HangHoa,@ID_LoHang
    					END
    				CLOSE Cur_tblKiemKe  
    				DEALLOCATE Cur_tblKiemKe 				
    			end
    
    			-- get luyke dauky of HangHoa not exist in ctkiemke
    			begin
    				insert into @LuyKeDauKy
    				select 
    					ID_LoHang,ID_HangHoa,TonDauKy											
    				from
    					(
    					select 
    						ID_LoHang,ID_HangHoa,TonDauKy,
    						ROW_NUMBER() OVER (PARTITION BY ID_HangHoa, ID_LoHang ORDER BY NgayLapHoaDon DESC) AS RN 
    					from #temp
    					where NgayLapHoaDon < @NgayLapHDMin 
    						and not exists (select * from @tblHangKiemKe kk where #temp.ID_HangHoa =  kk.ID_HangHoa and (#temp.ID_LoHang = kk.ID_LoHang OR #temp.ID_LoHang is null))
    					) luyke	
    				where luyke.RN= 1
    
    				-- caculator again TonLuyKe for all cthd 'liên quan'
    				INSERT INTO @hdctUpdate
    				select ID, ID_DonVi, ID_CheckIn,
    						ISNULL(lkdk.TonLuyKe, 0) + 
    						(SUM(IIF(LoaiHoaDon IN (1, 5, 7, 8), -1 * a.SoLuong* a.TyLeChuyenDoi, 
    					IIF(LoaiHoaDon IN (4, 6, 18,13,14), SoLuong * TyLeChuyenDoi, 				
    						IIF((LoaiHoaDon = 10 AND YeuCau = '1') OR (ID_CheckIn IS NOT NULL AND ID_CheckIn != @IDChiNhanhInput AND LoaiHoaDon = 10 AND YeuCau = '4') AND ID_DonVi = @IDChiNhanhInput, -1 * TienChietKhau* TyLeChuyenDoi, 				
    					IIF(a.LoaiHoaDon = 10 AND a.YeuCau = '4' AND a.ID_CheckIn = @IDChiNhanhInput, a.TienChietKhau* a.TyLeChuyenDoi, 0))))) 
    						OVER(PARTITION BY a.ID_HangHoa, a.ID_LoHang ORDER BY NgayLapHoaDon)) AS TonLuyKe,
    						LoaiHoaDon, MaHoaDon,NgayLapHoaDon,YeuCau
    				from
    					(
    					select distinct
    						ID,
    						ID_LoHang,
    						SoLuong,
    						TienChietKhau,
    						ThanhTien,
    						ID_HangHoa,
    						TyLeChuyenDoi,
    						MaHoaDon,
    						LoaiHoaDon,
    						NgayLapHoaDon,
    						ID_DonVi,
    						ID_CheckIn,
    						YeuCau
    					from #temp
    					where NgayLapHoaDon >= @NgayLapHDMin
    					and not exists (select * from @tblHangKiemKe kk where #temp.ID_HangHoa =  kk.ID_HangHoa and (#temp.ID_LoHang = kk.ID_LoHang OR #temp.ID_LoHang is null))
    					) a
    				LEFT JOIN @LuyKeDauKy lkdk ON lkdk.ID_HangHoa = a.ID_HangHoa AND (lkdk.ID_LoHang = a.ID_LoHang OR a.ID_LoHang IS NULL)					
    			end
    		
    		--select *, 1 as after2 from @LuyKeDauKy
    		--select * , @NgayLapHDMin as NgayMin from @hdctUpdate order by NgayLapHoaDon desc
    
    		UPDATE hdct
    	SET hdct.TonLuyKe = IIF(tlkupdate.ID_DonVi = @IDChiNhanhInput, tlkupdate.TonLuyKe, hdct.TonLuyKe), 
    		hdct.TonLuyKe_NhanChuyenHang = IIF(tlkupdate.ID_CheckIn = @IDChiNhanhInput and tlkupdate.LoaiHoaDon = 10, tlkupdate.TonLuyKe, hdct.TonLuyKe_NhanChuyenHang)
    	FROM BH_HoaDon_ChiTiet hdct
    	INNER JOIN @hdctUpdate tlkupdate ON hdct.ID = tlkupdate.ID where tlkupdate.LoaiHoaDon !=9 -- don't update TonLuyKe of HD KiemKe
    
    		-- get TonKho hientai full ID_QuiDoi, ID_LoHang of ID_HangHoa
    		DECLARE @tblTonKho1 TABLE(ID_DonViQuiDoi UNIQUEIDENTIFIER, TonKho FLOAT, ID_LoHang UNIQUEIDENTIFIER)
    		INSERT INTO @tblTonKho1
    		SELECT qd.ID, [dbo].[FUNC_TonLuyKeTruocThoiGian](@IDChiNhanhInput,qd.ID_HangHoa,ID_LoHang, DATEADD(HOUR, 1,GETDATE()))/qd.TyLeChuyenDoi as TonKho, ID_LoHang 
    		FROM @tblChiTiet ct
    		join DonViQuiDoi qd on ct.ID_HangHoa = qd.ID_HangHoa 
    		
    		--select * from @tblTonKho1
    
    		-- UPDATE TonKho in DM_HangHoa_TonKho
    		UPDATE hhtonkho SET hhtonkho.TonKho = ISNULL(cthoadon.TonKho, 0)
    		FROM DM_HangHoa_TonKho hhtonkho
    		INNER JOIN @tblTonKho1 as cthoadon on hhtonkho.ID_DonViQuyDoi = cthoadon.ID_DonViQuiDoi 
    			and (hhtonkho.ID_LoHang = cthoadon.ID_LoHang or cthoadon.ID_LoHang is null) and hhtonkho.ID_DonVi = @IDChiNhanhInput

		--- insert row into DM_HangHoa_TonKho if not exists
    INSERT INTO DM_HangHoa_TonKho(ID, ID_DonVi, ID_DonViQuyDoi, ID_LoHang, TonKho)
	SELECT NEWID(), @IDChiNhanhInput, cthoadon1.ID_DonViQuiDoi, cthoadon1.ID_LoHang, cthoadon1.TonKho
    FROM @tblTonKho1 AS cthoadon1
    LEFT JOIN DM_HangHoa_TonKho hhtonkho1 on hhtonkho1.ID_DonViQuyDoi = cthoadon1.ID_DonViQuiDoi 
	and (hhtonkho1.ID_LoHang = cthoadon1.ID_LoHang or cthoadon1.ID_LoHang is null) and hhtonkho1.ID_DonVi = @IDChiNhanhInput
	WHERE hhtonkho1.ID IS NULL

	
	exec Insert_ThongBaoHetTonKho @IDChiNhanhInput, @LoaiHoaDon, @tblHoaDonChiTiet

    
    		-- delete cthd was delete in cthd update
    		--delete from BH_HoaDon_ChiTiet where id in (select id from @tblChiTiet where ChatLieu='5') OR(ID_HoaDon = @IDHoaDonInput and ChatLieu='5')	
    		delete from BH_HoaDon_ChiTiet where ID_HoaDon = @IDHoaDonInput and ChatLieu='5'
    
    		-- neu update NhanHang --> goi ham update TonKho 2 lan
    		-- update GiaVon neu tontai phieu NhapHang,ChuyenHang/NhanHang, DieuChinhGiaVon 
    		declare @count2 float = (select count(ID) from @hdctUpdate where LoaiHoaDon in (4,7,10, 18))
    		select ISNULL(@count2,0) as UpdateGiaVon, ISNULL(@count,0) as UpdateKiemKe, @NgayLapHDMin as NgayLapHDMin

		
	


END");

			Sql(@"ALTER PROCEDURE [dbo].[XuatKhoToanBo_FromHoaDonSC]
    @ID_HoaDon [uniqueidentifier] 
AS
BEGIN
    SET NOCOUNT ON;

	-- count cthd la hanghoa
		declare @tongGtriXuat float, @count int
		select @count = count(ct.ID) , @tongGtriXuat= sum(ct.GiaVon * SoLuong)
			from BH_HoaDon_ChiTiet ct 
			join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
			join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
			where ct.ID_HoaDon= @ID_HoaDon
			and hh.LaHangHoa = 1 and (ct.ChatLieu is null or ct.ChatLieu !='5')
		
		IF @count > 0
		BEGIN
				---- INSERT HD XUATKHO ----
			
			 declare @ID_XuatKho uniqueidentifier = newID()	
			 declare @ngaylapHD datetime, @ID_DonVi uniqueidentifier,  @mahoadon nvarchar(max)
    		select @ngaylapHD = NgayLapHoaDon, @ID_DonVi = ID_DonVi from BH_HoaDon where id= @ID_HoaDon
    		declare @ngayxuatkho datetime = dateadd(millisecond,2,@ngaylapHD)

				---- get mahoadon xuatkho
    			declare @tblMa table (MaHoaDon nvarchar(max))
    			insert into @tblMa
    			exec GetMaHoaDonMax_byTemp 8, @ID_DonVi, @ngaylapHD
    			select @mahoadon = MaHoaDon from @tblMa

				insert into BH_HoaDon (ID, LoaiHoaDon, MaHoaDon, ID_HoaDon,ID_PhieuTiepNhan, NgayLapHoaDon, ID_DonVi, ID_NhanVien, TongTienHang, TongThanhToan, TongChietKhau, TongChiPhi, TongGiamGia, TongTienThue, 
    			PhaiThanhToan, PhaiThanhToanBaoHiem, ChoThanhToan, YeuCau, NgayTao, NguoiTao, DienGiai)
    			select @ID_XuatKho, 8, @mahoadon,@ID_HoaDon,ID_PhieuTiepNhan, @ngayxuatkho, @ID_DonVi,ID_NhanVien, @tongGtriXuat,0,0,0,0,0, @tongGtriXuat,0,0,N'Hoàn thành', GETDATE(), NguoiTao, 
				concat(N'Xuất kho toàn bộ từ hóa đơn ', MaHoaDon)
    			from BH_HoaDon 
    			where id= @ID_HoaDon

    
			---- INSERT CT XUATKHO -----
		
		
			insert into BH_HoaDon_ChiTiet (ID, ID_HoaDon, SoThuTu, ID_ChiTietDinhLuong, ID_ChiTietGoiDV, 
						ID_DonViQuiDoi, ID_LoHang, SoLuong, DonGia, GiaVon, ThanhTien, ThanhToan, 
    					PTChietKhau, TienChietKhau, PTChiPhi, TienChiPhi, TienThue, An_Hien, TonLuyKe, GhiChu, TenHangHoaThayThe, ChatLieu)				
			select 
				NEWid(),
				@ID_XuatKho,
				row_number() over( order by (select 1)) as SoThuTu,
				ctsc.ID_ChiTietDinhLuong,
				ctsc.ID_ChiTietGoiDV,
				ctsc.ID_DonViQuiDoi,
				ctsc.ID_LoHang,
				ctsc.SoLuong, ctsc.GiaVon, ctsc.GiaVon, ctsc.GiaTri, 
				0,0,0,0,0,0,'1', ctsc.TonLuyKe, ctsc.GhiChu, ctsc.TenHangHoaThayThe, ctsc.ChatLieu
			from 
			(
			--- ct hoadon suachua
				select 
					cttp.ID as ID_ChiTietGoiDV,
					isnull(ctdv.ID_DichVu,cttp.ID_DonViQuiDoi) as ID_ChiTietDinhLuong, -- id_hanghoa/id_dichvu
					cttp.SoLuong,
					cttp.GiaVon,
					cttp.GiaVon* cttp.SoLuong as GiaTri,
					cttp.ID_DonViQuiDoi,
					cttp.ID_LoHang,
					cttp.TonLuyKe,
					isnull(cttp.GhiChu,'') as GhiChu,
					isnull(cttp.TenHangHoaThayThe,'') as TenHangHoaThayThe,
					cttp.ChatLieu -- chatlieu = 4. check sudung gdv
				from BH_HoaDon_ChiTiet cttp
				left join
				(
					select ctm.ID_DonViQuiDoi as ID_DichVu, ctm.ID, ctm.ID_LichBaoDuong
					from BH_HoaDon_ChiTiet ctm where ctm.ID_HoaDon= @ID_HoaDon
					and ctm.SoLuong > 0
				) ctdv on cttp.ID_ChiTietDinhLuong = ctdv.ID
				where cttp.ID_HoaDon= @ID_HoaDon
				and cttp.SoLuong > 0
				and cttp.ID_LichBaoDuong is null ---- khong xuat hang bao duong
				) ctsc
				JOIN DonViQuiDoi qd on ctsc.ID_DonViQuiDoi = qd.ID
				JOIN DM_HangHoa hh on qd.ID_HangHoa = hh.ID
				where hh.LaHangHoa ='1'


			select @ID_XuatKho as ID_HoaDon,  @ngayxuatkho as NgayLapHoaDon ---- get ngaylaphd of hdsc --> insert diary & update tonkho

			--exec UpdateTonLuyKeCTHD_whenUpdate @ID_XuatKho,@ID_DonVi,@ngayxuatkho
		END
		
END");
        }
        
        public override void Down()
        {
            DropStoredProcedure("[dbo].[GetQuyHoaDon_byIDHoaDon]");
			DropStoredProcedure("[dbo].[GetInforTheGiaTri_byID]");
			DropStoredProcedure("[dbo].[GetThoiGianHoatDong_v1]");
			DropStoredProcedure("[dbo].[GetListPhuTungTheoDoi_v1]");
        }
    }
}
