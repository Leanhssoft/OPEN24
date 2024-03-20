namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20230308 : DbMigration
    {
        public override void Up()
        {
			Sql(@"CREATE FUNCTION [dbo].[BuTruTraHang_HDDoi]
(
	@ID_HoaDon uniqueidentifier,
	@NgayLapHoaDon datetime,
    @ID_HoaDonGoc uniqueidentifier = null,
	@LoaiHDGoc int =  0
)
RETURNS float
AS
BEGIN
	--select id, id_hoadon, ngaylaphoadon from BH_HoaDon where MaHoaDon='HDBL20071904512'
	---select mahoadon,id, id_hoadon, ngaylaphoadon from BH_HoaDon where MaHoaDon='TH0000000121'


	--select dbo.BuTruTraHang_HDDoi('6AD8B1BD-4633-4057-9307-1E8E6C2A23C4','2022-11-15 13:57:19.473')
	--select dbo.BuTruTraHang_HDDoi('6D0213DC-0ABD-4B14-AB6E-C882055025DD','2022-11-15 14:01:00.207') 
	--select dbo.BuTruTraHang_HDDoi('F606C546-0C22-4973-9082-D0D48F89BE54','2022-11-15 14:58:54.107') 
	--select dbo.BuTruTraHang_HDDoi('9421E33F-9476-4217-AA95-BD74AEFE38C5','2022-11-15 13:52:30.820') 

	------ goc HDBL20071904275
	--select dbo.BuTruTraHang_HDDoi('2AF0E9B9-4A5D-466D-A7D1-489A5AAD79B9','2022-11-15 09:09:42.417') 
	--select dbo.BuTruTraHang_HDDoi('955EA919-9D3D-4557-89D8-0A99C8D8062E','2022-11-15 09:10:42.623')
	--select dbo.BuTruTraHang_HDDoi('74E3118F-4449-4D0D-8E9D-B546220754FB','2022-11-15 09:11:48.777') 
	


	------ HDBL20071904511 (CN04)
	--select dbo.BuTruTraHang_HDDoi('DD0FDD2B-D914-4AC4-AD7B-94006C54FD75','2023-02-15 10:19:47.960') 
	--select dbo.BuTruTraHang_HDDoi('A7E3D8E1-A61D-4EAA-A036-BB2D76D9CE24','2023-02-15 10:20:39.100') --2023-02-15 10:19:38.943  10:19:47.960
	--select dbo.BuTruTraHang_HDDoi('FC373C3D-2B67-47E3-B832-BDA868F74819','2023-02-15 10:19:38.943') 10:19:47 (hd 512 > th 121
	
	
			
	DECLARE @Gtri float=0

	----- neu gtributru > 0 --> khong butru
	set @Gtri = (		
				select 
					sum(isnull(PhaiThanhToan,0) + isnull(DaThanhToan,0)) as BuTruTra
				from
				(
				select 		
					ID,
					
					iif(LoaiHoaDon =6, -PhaiThanhToan, PhaiThanhToan)  -- 0  as PhaiThanhToan
						+ isnull((select dbo.BuTruTraHang_HDDoi(ID_HoaDon, NgayLapHoaDon)),0) --- hdTra tructiep cua hdDoi + hdGoc
						+ isnull((select dbo.GetTongTra_byIDHoaDon(ID_HoaDon, NgayLapHoaDon)),0) --- allHDTra + chilienquan					
						as PhaiThanhToan,
					0 as DaThanhToan
				from BH_HoaDon
				where ChoThanhToan='0'
				and LoaiHoaDon in (1,6,19,25)
				and ID= @ID_HoaDon
				and NgayLapHoaDon < @NgayLapHoaDon

				

				union all

				-- get phieuthu/chi of hd (tra/hdgoc) duoc truyen vao ---
				
				select 
					qct.ID_HoaDonLienQuan,
					0 as PhaiThanhToan, 
					iif(qhd.LoaiHoaDon=12,  qct.TienThu, -qct.TienThu) as DaThanhToan
				from Quy_HoaDon_ChiTiet qct				
				join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
				where qhd.TrangThai='1'
				and qct.ID_HoaDonLienQuan= @ID_HoaDon				
			

				----- todo thudathang (if hdgoc)
				
				) tbl
		)
	
	RETURN @Gtri;
END");

			Sql(@"CREATE FUNCTION [dbo].[GetTongTra_byIDHoaDon]
(
	@ID_HoaDon uniqueidentifier,
	@NgayLapHoaDon datetime
)
RETURNS float
AS
BEGIN
		DECLARE @Gtri float=0

		---- get all hdTra of hdgoc with ngaylap < ngaylap of HDTra current --
		declare @tblHDTra table (ID uniqueidentifier, PhaiThanhToan float)
		insert into @tblHDTra
		select ID, PhaiThanhToan
		from BH_HoaDon
		where LoaiHoaDon = 6
		and ChoThanhToan='0'
		and ID_HoaDon= @ID_HoaDon
		and NgayLapHoaDon < @NgayLapHoaDon ---- hd$root: get allHDTra (don't check NgayLap)

	set @Gtri = (
					select 					
						sum(PhaiThanhToan + DaTraKhach) as NoKhach
					from
					(
						select ID, -PhaiThanhToan as PhaiThanhToan, 0 as DaTraKhach
						from @tblHDTra					

						union all
						---- get phieuchi hdTra ----
						select 
							hdt.ID,
							0 as PhaiThanhToan,
							iif(qhd.LoaiHoaDon=12, qct.TienThu, -qct.TienThu) as DaTraKhach
						from @tblHDTra hdt
						join Quy_HoaDon_ChiTiet qct on hdt.ID = qct.ID_HoaDonLienQuan
						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
						where qhd.TrangThai='1'
						and qhd.NgayLapHoaDon < @NgayLapHoaDon
					) tblThuChi
		)
	RETURN @Gtri 

END
");

			CreateStoredProcedure(name: "[dbo].[CaiDatTienDo_GetKhoangThoiGian]", parametersAction: p => new
			{
				ID_PhieuTiepNhan = p.Guid()
			}, body: @"SET NOCOUNT ON;

   ----- get time from tiepnhanxe --> time settup
	declare @ngayVaoxuong datetime =( select top 1 NgayTao from Gara_PhieuTiepNhan where id =@ID_PhieuTiepNhan)
	declare @toDate datetime

	---- get tổng thời gian cài đặt (từ lúc Tiếp nhận --> bước cuối cùng)
	----- get thong bao trong khoảng thời gian này ----
	declare @sumThoiGian int = ( select max(TongThoiGian)
								from (select 
									sum (iif(NhacTruocLoaiThoiGian=2, NhacTruocThoiGian * 60, NhacTruocThoiGian))		
									 OVER(ORDER BY LoaiThoiGianLapLai) as TongThoiGian		
										from HT_ThongBao_CatDatThoiGian
									)tbl)
	declare @dtNow datetime	= dateadd(day,1, getdate())
	set @toDate = DATEADD(MINUTE,@sumThoiGian + 30,@ngayVaoxuong)
	if @toDate < FORMAT(@dtNow,'yyyy-MM-dd')
		set @toDate = @dtNow
	select @ngayVaoxuong as FromDate, @toDate as ToDate");

			CreateStoredProcedure(name: "[dbo].[Gara_CheckTienDoCongViec]", parametersAction: p => new
			{
				ID_DonVi = p.Guid(),
				ID_PhieuTiepNhan = p.Guid(),
				BienSo = p.String(maxLength: 50),
				TimeCompare = p.DateTime(),
				TimeSetup = p.Int(),
				LoaiNhac = p.Int()
			}, body: @"SET NOCOUNT ON;

	declare @fromDate datetime, @toDate datetime
	set @fromDate = @TimeCompare
	set @toDate = DATEADD(minute,@TimeSetup +5, @TimeCompare)

		---- get all HD + baogia of PTN
	declare @tblHoaDon table(ID uniqueidentifier, LoaiHoaDon int, ID_PhieuTiepNhan uniqueidentifier, ID_Xe uniqueidentifier, NgayLapHoaDon datetime)
	insert into @tblHoaDon
	select hd.ID, hd.LoaiHoaDon, hd.ID_PhieuTiepNhan, hd.ID_Xe, hd.NgayLapHoaDon
	from BH_HoaDon hd
	where hd.ChoThanhToan ='0'
	and hd.LoaiHoaDon in (25,3)
	and hd.ID_PhieuTiepNhan = @ID_PhieuTiepNhan	
	
		declare @isInsert bit ='0'
		declare @loaiThongBao int= @LoaiNhac + 4

		declare @noidung nvarchar(max)=  CONCAT(N'<p onclick=""loaddadoc(''key'')"">',
				N' Xe <a onclick=""loadthongbao(''', @loaiThongBao, ''', ''', @BienSo, N''', ''key'')""> ', @BienSo, N' </a> ')


		if DATEDIFF(minute, @TimeCompare, GETDATE()) >= @TimeSetup
		begin
			if @LoaiNhac = 1
			begin
				if not exists(select tb.ID
									from HT_ThongBao tb
									where tb.LoaiThongBao = @loaiThongBao-- loai5: nhac tiendo
									and tb.NgayTao between @fromDate and @toDate
									and tb.NoiDungThongBao like N'%' + @BienSo + '%'
									and tb.NoiDungThongBao like N'%chưa báo giá%'
						)
					and not exists(select *
						from @tblHoaDon
						where LoaiHoaDon = 3)

					begin
						set @noidung = CONCAT(@noidung, N'chưa báo giá </p>')

						insert into HT_ThongBao(ID, ID_DonVi, LoaiThongBao, NoiDungThongBao, NgayTao, NguoiDungDaDoc)
						values(newID(), @ID_DonVi, @loaiThongBao, @noidung, GETDATE(), '')

						set @isInsert = '1'
					end
			end

			if @LoaiNhac = 2---- chua datcoc
			begin
				if not exists(select tb.ID
									from HT_ThongBao tb
									where tb.LoaiThongBao = @loaiThongBao
									and tb.NgayTao between @fromDate and @toDate
									and tb.NoiDungThongBao like N'%' + @BienSo + '%'
									and tb.NoiDungThongBao like N'%chưa đặt cọc%'
						)
					and not exists(
						select hd.*
						from @tblHoaDon hd
						join Quy_HoaDon_ChiTiet qct on hd.ID = qct.ID_HoaDonLienQuan
						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
						where qhd.TrangThai = '1'
						and hd.LoaiHoaDon = 3
					)
					begin
						set @noidung = CONCAT(@noidung, N'chưa đặt cọc </p>')


						insert into HT_ThongBao(ID, ID_DonVi, LoaiThongBao, NoiDungThongBao, NgayTao, NguoiDungDaDoc)
						values(newID(), @ID_DonVi, @loaiThongBao, @noidung, GETDATE(), '')


						set @isInsert = '1'
					end
			end

			if @LoaiNhac = 3---- chua tao hoadon
		   begin
				if not exists(select tb.ID
									from HT_ThongBao tb
									where tb.LoaiThongBao = @loaiThongBao
									and tb.NgayTao between @fromDate and @toDate
									and tb.NoiDungThongBao like N'%' + @BienSo + '%'
									and tb.NoiDungThongBao like N'%chưa tạo lệnh sửa chữa%'
						)
					and not exists(select *
						from @tblHoaDon
						where LoaiHoaDon = 25)

					begin
						set @noidung = CONCAT(@noidung, N'chưa tạo lệnh sửa chữa </p>')

						insert into HT_ThongBao(ID, ID_DonVi, LoaiThongBao, NoiDungThongBao, NgayTao, NguoiDungDaDoc)
						values(newID(), @ID_DonVi, @loaiThongBao, @noidung, GETDATE(), '')

						set @isInsert = '1'
					end
			end

			if @LoaiNhac = 4---- chua thanhtoan
			begin
				if not exists(select tb.ID
									from HT_ThongBao tb
									where tb.LoaiThongBao = @loaiThongBao
									and tb.NgayTao between @fromDate and @toDate
									and tb.NoiDungThongBao like N'%' + @BienSo + '%'
									and tb.NoiDungThongBao like N'%chưa thanh toán%'
						)
					and not exists(
						select hd.*
						from @tblHoaDon hd
						join Quy_HoaDon_ChiTiet qct on hd.ID = qct.ID_HoaDonLienQuan
						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
						where qhd.TrangThai = '1'
						and hd.LoaiHoaDon = 25
					)
					begin
						set @noidung = CONCAT(@noidung, N'chưa thanh toán </p>')


						insert into HT_ThongBao(ID, ID_DonVi, LoaiThongBao, NoiDungThongBao, NgayTao, NguoiDungDaDoc)
						values(newID(), @ID_DonVi, @loaiThongBao, @noidung, GETDATE(), '')


						set @isInsert = '1'
					end
			end

			if @LoaiNhac = 5---- chua xuatkho
			begin

				if not exists(select tb.ID
									from HT_ThongBao tb
									where tb.LoaiThongBao = @loaiThongBao
									and tb.NgayTao between @fromDate and @toDate
									and tb.NoiDungThongBao like N'%' + @BienSo + '%'
									and tb.NoiDungThongBao like N'%chưa xuất kho%'
						)
					and not exists(
						select hd.*
						from @tblHoaDon hd
						join BH_HoaDon hdx on hd.ID = hdx.ID_HoaDon
						where hdx.ChoThanhToan = '0' and hdx.LoaiHoaDon = 8
					)
					begin
						set @noidung = CONCAT(@noidung, N'chưa xuất kho </p>')


						insert into HT_ThongBao(ID, ID_DonVi, LoaiThongBao, NoiDungThongBao, NgayTao, NguoiDungDaDoc)
						values(newID(), @ID_DonVi, @loaiThongBao, @noidung, GETDATE(), '')


						set @isInsert = '1'
					end
			end

			if @LoaiNhac = 6---- chua xuatxuong
			begin
				if not exists(select tb.ID
									from HT_ThongBao tb
									where tb.LoaiThongBao = @loaiThongBao
									and tb.NgayTao between @fromDate and @toDate
									and tb.NoiDungThongBao like N'%' + @BienSo + '%'
									and tb.NoiDungThongBao like N'%chưa xuất xưởng%'
						)
					and not exists(
						select tn.ID
						from Gara_PhieuTiepNhan tn
						where tn.ID = @ID_PhieuTiepNhan
						and tn.NgayXuatXuong is null
					)
					begin
						set @noidung = CONCAT(@noidung, N'chưa xuất xưởng </p>')


						insert into HT_ThongBao(ID, ID_DonVi, LoaiThongBao, NoiDungThongBao, NgayTao, NguoiDungDaDoc)
						values(newID(), @ID_DonVi, @loaiThongBao, @noidung, GETDATE(), '')


						set @isInsert = '1'
					end
			end
		end


		select @isInsert as ThongBao");

			CreateStoredProcedure(name: "[dbo].[GetHoaDonBaoGia_ofXeDangSua]", parametersAction: p => new
			{
				IDChiNhanhs = p.String(),
				ID_PhieuTiepNhan = p.String(),
				TextSearch = p.String()
			}, body: @"SET NOCOUNT ON;

		select  hd.ID, tn.ID_Xe, hd.MaHoaDon, hd.ID_PhieuTiepNhan, hd.NgayLapHoaDon, tn.MaPhieuTiepNhan, xe.BienSo		
    	from BH_HoaDon hd		
		join Gara_PhieuTiepNhan tn on hd.ID_PhieuTiepNhan = tn.ID
		join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
		where hd.LoaiHoaDon in (3,25)
		and exists (select Name from dbo.splitstring(@IDChiNhanhs) dv where dv.Name= hd.ID_DonVi )
    	and hd.ID_PhieuTiepNhan is not null
    	and hd.ChoThanhToan= 0
		and hd.ID_PhieuTiepNhan like @ID_PhieuTiepNhan
		and tn.TrangThai in (1,2)
    	and (tn.MaPhieuTiepNhan like @TextSearch 
    		or xe.BienSo like @TextSearch 		
    		or hd.MaHoaDon like @TextSearch 		
    		)	
		order by tn.NgayVaoXuong desc , hd.NgayLapHoaDon desc");

			CreateStoredProcedure(name: "[dbo].[GetLichNhacBaoDuong_TheoXe]", parametersAction: p => new
			{
				IDChiNhanhs = p.String(),
				ID_Xe = p.String(),
				TextSearch = p.String(),
				NgayBaoDuongFrom = p.DateTime(),
				NgayBaoDuongTo = p.DateTime(),
				TrangThais = p.String(maxLength: 20),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;

		declare @tblChiNhanh table(ID uniqueidentifier)
		if isnull(@IDChiNhanhs,'')!=''
			begin
				insert into @tblChiNhanh
				select name from dbo.splitstring(@IDChiNhanhs) where Name!=''
			end

		declare @tblTrangThai table(TrangThai int)
		if isnull(@TrangThais,'')!=''
			begin
				set @TrangThais =''
				insert into @tblTrangThai
				select name from dbo.splitstring(@TrangThais)
			end

		if isnull(@ID_Xe,'')=''
			set @ID_Xe =''

		--declare @tblSearch table(ID nvarchar(max))
		--if isnull(@TextSearch,'')!=''
		--	insert into @tblSearch
		--	select name from dbo.splitstring(@TextSearch)
	
		if isnull(@CurrentPage,0)=0
			set @CurrentPage= 0
		if isnull(@PageSize,0)=0
			set @PageSize= 10


	;with data_cte
	as(
		select lich.ID, 
			lich.ID_Xe, 
			lich.NgayBaoDuongDuKien,
			lich.SoKmBaoDuong,
			lich.LanBaoDuong,
			lich.LanNhac,
			lich.GhiChu,
			lich.TrangThai,
			xe.BienSo,
			xe.ID_KhachHang as ID_DoiTuong,
			dt.MaDoiTuong,
			dt.TenDoiTuong,
			dt.DienThoai,
			dt.Email			
		from Gara_LichBaoDuong lich
		join Gara_DanhMucXe xe on lich.ID_Xe= xe.ID
		left join DM_DoiTuong dt on xe.ID_KhachHang = dt.ID
		left join BH_HoaDon hd on lich.ID_HoaDon= hd.ID
		where (@IDChiNhanhs='' or exists (select ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID))
		and (@NgayBaoDuongFrom is null or lich.NgayBaoDuongDuKien between @NgayBaoDuongFrom and @NgayBaoDuongTo)
		and (@TrangThais ='' or exists (select tt.TrangThai from @tblTrangThai tt where lich.TrangThai = tt.TrangThai))
		and (@TextSearch='' or (
			dt.MaDoiTuong like N'%'+ @TextSearch +'%'
			or dt.TenDoiTuong like N'%'+ @TextSearch +'%'
			or dt.TenDoiTuong_KhongDau like N'%'+ @TextSearch +'%'
			or dt.DienThoai like N'%'+ @TextSearch +'%'
			or xe.BienSo like N'%'+ @TextSearch +'%'
			or lich.GhiChu like N'%'+ @TextSearch +'%'
			))
		and lich.ID_Xe like N'%'+ @ID_Xe +'%'
	),
	count_cte
	as
	(
		select count(ID) as TotalRow,
		 CEILING(count(ID)/ cast(@PageSize as float)) as TotalPage
		from data_cte dt
	)
	select *
	from data_cte dt
	cross join count_cte
	order by dt.NgayBaoDuongDuKien 
	OFFSET (@CurrentPage* @PageSize) ROWS
	FETCH NEXT @PageSize ROWS ONLY");

			CreateStoredProcedure(name: "[dbo].[GetListThongBao_ChuaDoc]", parametersAction: p => new
			{
				IdDonVi = p.Guid(),
				IdNguoiDung = p.String(),
				NhacSinhNhat = p.Boolean(),
				NhacTonKho = p.Boolean(),
				NhacDieuChuyen = p.Boolean(),
				NhacLoHang = p.Boolean(),
				NhacBaoDuong = p.Boolean(),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;
    
    -- Insert statements for procedure here
    	DECLARE @tblCaiDat TABLE (GiaTri INT);
    	IF @NhacBaoDuong = 1
    	BEGIN
    		INSERT INTO @tblCaiDat VALUES (4);
    	END
    	IF @NhacDieuChuyen = 1
    	BEGIN
    		INSERT INTO @tblCaiDat VALUES (1);
    	END
    	IF @NhacLoHang = 1
    	BEGIN
    		INSERT INTO @tblCaiDat VALUES (2);
    	END
    	IF @NhacSinhNhat = 1
    	BEGIN
    		INSERT INTO @tblCaiDat VALUES (3);
    	END
    	IF @NhacTonKho = 1
    	BEGIN
    		INSERT INTO @tblCaiDat VALUES (0);
    	END

		INSERT INTO @tblCaiDat VALUES (5),(6),(7),(8),(9),(10);---- 5. bacogia, 6.coc, 7.hoadon, 8.thanhtoan, 9.xuatxuong
      
    
    	;with data_cte
    	AS
    	(SELECT tb.ID, tb.ID_DonVi, tb.LoaiThongBao, tb.NoiDungThongBao, tb.NgayTao, tb.NguoiDungDaDoc 
		FROM HT_ThongBao tb
    	INNER JOIN @tblCaiDat cd ON tb.LoaiThongBao = cd.GiaTri
    	WHERE ID_DonVi = @IdDonVi
		and tb.NguoiDungDaDoc =''
		),
    	count_cte
    	as
    	(
    		select count(ID) as TotalRow,
    		CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage
    		from data_cte
    	)
    	SELECT dt.*, TotalRow AS ChuaDoc
		FROM data_cte dt
    	CROSS JOIN count_cte ct
    	ORDER BY dt.NgayTao desc
    	OFFSET (@CurrentPage * @PageSize) ROWS
    	FETCH NEXT @PageSize ROWS ONLY;");

			CreateStoredProcedure(name: "[dbo].[Insert_ThongBaoBaoDuong_TheoXe]", body: @"SET NOCOUNT ON;

		declare @dtNow varchar(14) = format(getdate(),'yyyy-MM-dd')
		declare @dtNowTo varchar(14) = format(dateadd(day,1, getdate()),'yyyy-MM-dd')


		declare @isLeeAuto bit = '0'
		if (select count(*) from HT_ThongBao_CatDatThoiGian where LoaiThongBao = 0) > 0
			set @isLeeAuto = '1'

		if @isLeeAuto ='1'
			begin
				declare @tblLichBaoDuong table(ID uniqueidentifier, ID_DonVi uniqueidentifier,BienSo nvarchar(max), Ten_DienThoai nvarchar(max))
				insert into @tblLichBaoDuong
				select newID(),
					ID_DonVi,
					tbl.BienSo,
					tbl.Ten_DienThoai
				from
				(
				select lich.ID,lich.ID_HoaDon,  xe.BienSo, dt.TenDoiTuong, dt.DienThoai,
					iif(dt.DienThoai!='' and dt.DienThoai is not null, 
						CONCAT(dt.TenDoiTuong,' - ', dt.DienThoai), dt.TenDoiTuong) as Ten_DienThoai
				from Gara_LichBaoDuong lich
				join Gara_DanhMucXe xe on lich.ID_Xe = xe.ID
				left join DM_DoiTuong dt on xe.ID_KhachHang= dt.ID
				where lich.NgayBaoDuongDuKien between @dtNow and @dtNowTo
				and lich.TrangThai=1
				) tbl
				left join BH_HoaDon hd on tbl.ID_HoaDon = hd.ID


				insert into HT_ThongBao (ID, ID_DonVi, LoaiThongBao, NoiDungThongBao, NguoiDungDaDoc, NgayTao)
				select ID, ID_DonVi,
					4 as LoaiThongBao,
					concat(N'<p onclick=""loaddadoc(''', convert(nvarchar(50), ID) ,N''')"">Xe <a href="" /#/Gara_LichNhacBaoDuong?s=',
					BienSo + '&t=1"">', BienSo + N'</a> ', N' đến lịch bảo dưỡng. <br /> Chủ xe: <b>', Ten_DienThoai, ' </b> </p>'),
					'' as NguoiDungDaDoc,
					getdate() as NgayTao
				from @tblLichBaoDuong
			end");

			CreateStoredProcedure(name: "[dbo].[UpdateThongBao_CongViecDaXuLy]", parametersAction: p => new
			{
				ID_NguoiDung = p.Guid(),
				ID_PhieuTiepNhan = p.Guid(),
				BienSo = p.String(),
				LoaiThongBao = p.Int()
			}, body: @"SET NOCOUNT ON;

	set @LoaiThongBao = @LoaiThongBao + 4

    declare @tblFromTo table(FromDate datetime, ToDate datetime)
	insert into @tblFromTo
	exec CaiDatTienDo_GetKhoangThoiGian @ID_PhieuTiepNhan

	declare @fromDate datetime, @toDate datetime
	select top 1 @fromDate = FromDate, @toDate = ToDate from @tblFromTo


	----- update nguoidungdadoc -----
	declare @tblThongBao table(ID uniqueidentifier)
	insert into @tblThongBao
	select tb.ID
	from HT_ThongBao tb
	where tb.LoaiThongBao = @LoaiThongBao
	and tb.NgayTao between @fromDate and @toDate
	and tb.NoiDungThongBao like N'%'+  @BienSo + '%'
	and tb.NguoiDungDaDoc =''

	declare @count int= (select count(*) from @tblThongBao)

	if (@count > 0)
		begin
			update tb set tb.NguoiDungDaDoc = @ID_NguoiDung
			from HT_ThongBao tb
			where exists (select * from @tblThongBao tbl where tb.ID = tbl.ID)
		end

	select @count");


            Sql(@"ALTER PROCEDURE [dbo].[BaoCaoTaiChinh_CongNo_v2]
    @TextSearch [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [uniqueidentifier],
    @loaiKH [nvarchar](max),
    @ID_NhomDoiTuong [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);
    
    DECLARE @timeChotSo Datetime
    	Select @timeChotSo =  (Select NgayChotSo FROM ChotSo where ID_DonVi = @ID_ChiNhanh);
    		IF @timeChotSo != null
    		BEGIN
    			IF @timeChotSo < @timeStart
    			BEGIN
    		 SELECT 
    				MAX(dt.TenNhomDoiTuongs) as NhomDoiTac,
    			MAX(b.MaKhachHang) as MaDoiTac,
    			MAX(b.TenKhachHang) as TenDoiTac,
    			Case when (b.NoDauKy >= 0) then b.NoDauKy else 0 end as PhaiThuDauKy,
    			Case when (b.NoDauKy < 0) then b.NoDauKy *(-1) else 0 end as PhaiTraDauKy,
    			MAX(b.TongTienChi) as TongTienChi, 
    			MAX(b.TongTienThu) as TongTienThu,
    			Case when (b.NoCuoiKy >= 0) then b.NoCuoiKy else 0 end as PhaiThuCuoiKy,
    			Case when (b.NoCuoiKy < 0) then b.NoCuoiKy *(-1) else 0 end as PhaiTraCuoiKy
    	 FROM
    	(
    	  SELECT a.ID_KhachHang, 
    		  dt.MaDoiTuong AS MaKhachHang, 
    		  dt.TenDoiTuong AS TenKhachHang,
    		  a.NoDauKy,
    		  a.GhiNo As TongTienChi,
    		  a.GhiCo As TongTienThu,
    		  CAST(ROUND(a.NoDauKy + a.GhiNo - a.GhiCo, 0) as float) as NoCuoiKy,
    		  Case When dtn.ID_NhomDoiTuong is null then
    		  '00000000-0000-0000-0000-000000000000'
    			  else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    			  dt.LoaiDoiTuong
    	  FROM
    	  (
    	  SELECT HangHoa.ID_KhachHang,
    		SUM(HangHoa.NoDauKy) as NoDauKy, 
    		SUM(HangHoa.GhiNo) as GhiNo,
    		SUM(HangHoa.GhiCo) as GhiCo
    		FROM
    		(
    			SELECT
    				td.ID_DoiTuong AS ID_KhachHang,
    				SUM(td.CongNo) + SUM(td.DoanhThu) + SUM(td.TienChi) - SUM(td.TienThu) - SUM(td.GiaTriTra) AS NoDauKy,
    				0 AS GhiNo,
    				0 AS GhiCo
    			FROM
    			(
    			-- Chốt sổ
    				SELECT 
    					ID_KhachHang As ID_DoiTuong,
    					CongNo,
    					0 AS GiaTriTra,
    					0 AS DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi
    				FROM ChotSo_KhachHang
    				where ID_DonVi = @ID_ChiNhanh
    				UNION ALL
    			-- Doanh thu từ bán hàng từ ngày chốt sổ tới thời gian bắt đầu
    			SELECT 
    				bhd.ID_DoiTuong,
    				0 AS CongNo,
    				0 AS GiaTriTra,
    				SUM(bhd.TongThanhToan) AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,22,25) AND bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeChotSo AND bhd.NgayLapHoaDon < @timeStart
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
				--chi phí hóa đơn
					UNION ALL
						SELECT 
    						cp.ID_NhaCungCap,
							0 AS CongNo,
    						SUM(cp.ThanhTien) AS GiaTriTra,
    						0 AS DoanhThu,
    						0 AS TienThu,
    						0 AS TienChi
    					FROM BH_HoaDon bhd
						INNER JOIN BH_HoaDon_ChiPhi cp ON cp.ID_HoaDon = bhd.ID
    					WHERE bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeChotSo AND bhd.NgayLapHoaDon < @timeStart
    					AND bhd.ID_DonVi = @ID_ChiNhanh
    						AND cp.ID_NhaCungCap not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    					GROUP BY cp.ID_NhaCungCap
    			-- gia tri trả từ bán hàng
    			UNION All
    			SELECT bhd.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    				SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE (bhd.LoaiHoaDon = '6' OR bhd.LoaiHoaDon = '4') AND bhd.ChoThanhToan = 'false'  AND bhd.NgayLapHoaDon >= @timeChotSo AND bhd.NgayLapHoaDon < @timeStart
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- sổ quỹ
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				SUM(qhdct.TienThu) AS TienThu,
    				0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID -- thêm thẻ
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai != '0' OR qhd.TrangThai is null)  AND qhd.NgayLapHoaDon >= @timeChotSo AND qhd.NgayLapHoaDon < @timeStart
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND (bhd.LoaiHoaDon is null or bhd.LoaiHoaDon != 22)
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeChotSo AND qhd.NgayLapHoaDon < @timeStart
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    			) AS td
    		    GROUP BY td.ID_DoiTuong
    			UNION ALL
    				-- Công nợ phát sinh trong khoảng thời gian truy vấn
    			SELECT
    				pstv.ID_DoiTuong AS ID_KhachHang,
    				0 AS NoDauKy,
    				SUM(pstv.DoanhThu) + SUM(pstv.TienChi) AS GhiNo,
    				SUM(pstv.TienThu) + SUM(pstv.GiaTriTra) AS GhiCo
    			FROM
    			(
    			SELECT 
    				bhd.ID_DoiTuong,
    				0 AS GiaTriTra,
    				SUM(bhd.TongThanhToan) AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,22,25)  AND bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
				--Chi phí hóa đơn sửa chữa
				UNION ALL
				SELECT 
    				cp.ID_NhaCungCap,
    				0 AS GiaTriTra,
    				SUM(cp.ThanhTien) AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
				INNER JOIN BH_HoaDon_ChiPhi cp ON cp.ID_HoaDon = bhd.ID
    			WHERE bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND cp.ID_NhaCungCap not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY cp.ID_NhaCungCap
    			-- gia tri trả từ bán hàng
    			UNION All
    			SELECT bhd.ID_DoiTuong AS ID_KhachHang,
    				SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE (bhd.LoaiHoaDon = '6' OR bhd.LoaiHoaDon = '4') AND bhd.ChoThanhToan = 'false'  AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- sổ quỹ
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				SUM(qhdct.TienThu) AS TienThu,
    				0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID -- thêm thẻ
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				--AND (bhd.LoaiHoaDon is null or bhd.LoaiHoaDon != 22)
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    			) AS pstv
    		    GROUP BY pstv.ID_DoiTuong
    		)AS HangHoa
    				GROUP BY HangHoa.ID_KhachHang
    				) a
    				LEFT join DM_DoiTuong dt on a.ID_KhachHang = dt.ID
    					left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    				where dt.TheoDoi='0'
    				and dt.loaidoituong in (select * from splitstring(@loaiKH)) 
    					AND ((select count(Name) from @tblSearch b where     			
    		dt.MaDoiTuong like '%'+b.Name+'%'
    		or dt.TenDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    			or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		)=@count or @count=0)
    				) b
    				LEFT JOin DM_DoiTuong dt on b.ID_KhachHang = dt.ID
    				where b.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) or b.LoaiDoiTuong = 3 or @ID_NhomDoiTuong = ''
    			Group by b.ID_KhachHang, dt.LoaiDoiTuong, b.NoDauKy, b.TongTienChi, b.TongTienThu, b.NoCuoiKy
    		ORDER BY MAX(b.MaKhachHang) DESC
    			END
    			ELSE IF @timeChotSo > @timeEnd
    			BEGIN
    				SELECT 
    		MAX(dt.TenNhomDoiTuongs) as NhomDoiTac,
    		MAX(b.MaKhachHang) as MaDoiTac,
    		MAX(b.TenKhachHang) as TenDoiTac,
    			Case when (b.NoDauKy >= 0) then b.NoDauKy else 0 end as PhaiThuDauKy,
    			Case when (b.NoDauKy < 0) then b.NoDauKy *(-1) else 0 end as PhaiTraDauKy,
    		MAX(b.TongTienChi) as TongTienChi, 
    			MAX(b.TongTienThu) as TongTienThu,
    			Case when (b.NoCuoiKy >= 0) then b.NoCuoiKy else 0 end as PhaiThuCuoiKy,
    			Case when (b.NoCuoiKy < 0) then b.NoCuoiKy *(-1) else 0 end as PhaiTraCuoiKy
    	 FROM
    	(
    	  SELECT a.ID_KhachHang, 
    	  dt.MaDoiTuong AS MaKhachHang, 
    	  dt.TenDoiTuong AS TenKhachHang,
    		  a.NoDauKy,
    		  a.GhiNo As TongTienChi,
    		  a.GhiCo As TongTienThu,
    		  CAST(ROUND(a.NoDauKy + a.GhiNo - a.GhiCo, 0) as float) as NoCuoiKy,
    	  Case When dtn.ID_NhomDoiTuong is null then
    		  '00000000-0000-0000-0000-000000000000'
    			  else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    			  dt.LoaiDoiTuong
    	  FROM
    	  (
    	  SELECT HangHoa.ID_KhachHang,
    		SUM(HangHoa.NoDauKy) as NoDauKy, 
    		SUM(HangHoa.GhiNo) as GhiNo,
    		SUM(HangHoa.GhiCo) as GhiCo,
    		SUM(HangHoa.NoCuoiKy) as NoCuoiKy
    		FROM
    		(
    			SELECT
    			td.ID_DoiTuong AS ID_KhachHang,
    			0 AS NoDauKy,
    			0 AS GhiNo,
    			0 AS GhiCo,
    			SUM(td.CongNo) - SUM(td.DoanhThu) - SUM(td.TienChi) + SUM(td.TienThu) + SUM(td.GiaTriTra) AS NoCuoiKy
    			FROM
    			(
    			-- Chốt sổ
    			SELECT 
    			ID_KhachHang As ID_DoiTuong,
    			CongNo,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM ChotSo_KhachHang
    			where ID_DonVi = @ID_ChiNhanh
    			UNION ALL
    				-- Doanh thu từ bán hàng từ ngày chốt sổ tới thời gian bắt đầu
    			SELECT 
    			bhd.ID_DoiTuong,
    			0 AS CongNo,
    			0 AS GiaTriTra,
    			SUM(bhd.TongThanhToan) AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,22,25) AND bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeEnd AND bhd.NgayLapHoaDon < @timeChotSo
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- gia tri trả từ bán hàng
    			UNION All
    			SELECT bhd.ID_DoiTuong AS ID_KhachHang,
    			0 AS CongNo,
    			SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE (bhd.LoaiHoaDon = '6' OR bhd.LoaiHoaDon = '4') AND bhd.ChoThanhToan = 'false'  AND bhd.NgayLapHoaDon >= @timeEnd AND bhd.NgayLapHoaDon < @timeChotSo
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- sổ quỹ
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    			0 AS CongNo,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			SUM(qhdct.TienThu) AS TienThu,
    			0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID -- thêm thẻ
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai != '0' OR qhd.TrangThai is null)  AND qhd.NgayLapHoaDon >= @timeEnd AND qhd.NgayLapHoaDon < @timeChotSo
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				--AND (bhd.LoaiHoaDon is null or bhd.LoaiHoaDon != 22)
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    			0 AS CongNo,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeEnd AND qhd.NgayLapHoaDon < @timeChotSo
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    			) AS td
    		    GROUP BY td.ID_DoiTuong
    			UNION ALL
    				-- Công nợ phát sinh trong khoảng thời gian truy vấn
    			SELECT
    			pstv.ID_DoiTuong AS ID_KhachHang,
    			0 AS NoDauKy,
    			SUM(pstv.DoanhThu) + SUM(pstv.TienChi) AS GhiNo,
    			SUM(pstv.TienThu) + SUM(pstv.GiaTriTra) AS GhiCo,
    			0 AS NoCuoiKy
    			FROM
    			(
    			SELECT 
    			bhd.ID_DoiTuong,
    			0 AS GiaTriTra,
    			SUM(bhd.TongThanhToan) AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,22,25) AND bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- gia tri trả từ bán hàng
    			UNION All
    			SELECT bhd.ID_DoiTuong AS ID_KhachHang,
    			SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE (bhd.LoaiHoaDon = '6' OR bhd.LoaiHoaDon = '4') AND bhd.ChoThanhToan = 'false'  AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- sổ quỹ
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			SUM(qhdct.TienThu) AS TienThu,
    			0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID -- thêm thẻ
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				--AND (bhd.LoaiHoaDon is null or bhd.LoaiHoaDon != 22)
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    			) AS pstv
    		    GROUP BY pstv.ID_DoiTuong
    			)AS HangHoa
    			  	-- LEFT join DM_DoiTuong dt on HangHoa.ID_KhachHang = dt.ID
    				GROUP BY HangHoa.ID_KhachHang
    				) a
    				LEFT join DM_DoiTuong dt on a.ID_KhachHang = dt.ID
    					left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    				where dt.TheoDoi = '0'
    				and dt.loaidoituong in (select * from splitstring(@loaiKH)) 
    					AND ((select count(Name) from @tblSearch b where     			
    		dt.MaDoiTuong like '%'+b.Name+'%'
    		or dt.TenDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    			or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		)=@count or @count=0)
    				) b
    			LEFT JOin DM_DoiTuong dt on b.ID_KhachHang = dt.ID
    				where b.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) or b.LoaiDoiTuong = 3 or @ID_NhomDoiTuong = ''
    			Group by b.ID_KhachHang, dt.LoaiDoiTuong, b.NoDauKy, b.TongTienChi, b.TongTienThu, b.NoCuoiKy
    			ORDER BY MAX(b.MaKhachHang) DESC
    			END
    			ELSE
    			BEGIN
    			SELECT 
    			 MAX(dt.TenNhomDoiTuongs) as NhomDoiTac,
    		MAX(b.MaKhachHang) as MaDoiTac,
    		MAX(b.TenKhachHang) as TenDoiTac,
    			Case when (b.NoDauKy >= 0) then b.NoDauKy else 0 end as PhaiThuDauKy,
    			Case when (b.NoDauKy < 0) then b.NoDauKy *(-1) else 0 end as PhaiTraDauKy,
    		MAX(b.TongTienChi) as TongTienChi, 
    			MAX(b.TongTienThu) as TongTienThu,
    			Case when (b.NoCuoiKy >= 0) then b.NoCuoiKy else 0 end as PhaiThuCuoiKy,
    			Case when (b.NoCuoiKy < 0) then b.NoCuoiKy *(-1) else 0 end as PhaiTraCuoiKy
    	 FROM
    	(
    	  SELECT a.ID_KhachHang, 
    	  dt.MaDoiTuong AS MaKhachHang, 
    	  dt.TenDoiTuong AS TenKhachHang,
    		  a.NoDauKy,
    		  a.GhiNo As TongTienChi,
    		  a.GhiCo As TongTienThu,
    		  CAST(ROUND(a.NoDauKy + a.GhiNo - a.GhiCo,0) as float) as NoCuoiKy,
    	  Case When dtn.ID_NhomDoiTuong is null then
    		  '00000000-0000-0000-0000-000000000000'
    			  else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    			  dt.LoaiDoiTuong
    	  FROM
    	  (
    	  SELECT HangHoa.ID_KhachHang,
    		SUM(HangHoa.NoDauKy) as NoDauKy, 
    			SUM(HangHoa.GhiNo) as GhiNo,
    			SUM(HangHoa.GhiCo) as GhiCo,
    			SUM(HangHoa.NoCuoiKy) as NoCuoiKy
    		FROM
    		(
    			SELECT
    			td.ID_DoiTuong AS ID_KhachHang,
    			SUM(td.CongNo) - SUM(td.DoanhThu) - SUM(td.TienChi) + SUM(td.TienThu) + SUM(td.GiaTriTra) AS NoDauKy,
    				SUM(td.DoanhThu) + SUM(td.TienChi) AS GhiNo,
    				SUM(td.TienThu) + SUM(td.GiaTriTra) AS GhiCo,
    				0 AS NoCuoiKy
    			FROM
    			(
    			-- Chốt sổ
    				SELECT 
    				ID_KhachHang As ID_DoiTuong,
    				CongNo AS CongNo,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    				FROM ChotSo_KhachHang
    				where ID_DonVi = @ID_ChiNhanh
    				UNION ALL
    				-- Doanh thu từ bán hàng từ ngày chốt sổ tới thời gian bắt đầu
    			SELECT 
    			bhd.ID_DoiTuong,
    				0 AS CongNo,
    			0 AS GiaTriTra,
    			SUM(bhd.TongThanhToan) AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,25) AND bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeChotSo
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- gia tri trả từ bán hàng
    			UNION All
    			SELECT bhd.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    			SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE (bhd.LoaiHoaDon = '6' OR bhd.LoaiHoaDon = '4') AND bhd.ChoThanhToan = 'false'  AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeChotSo
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- sổ quỹ
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			SUM(qhdct.TienThu) AS TienThu,
    			0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID -- thêm thẻ
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai != '0' OR qhd.TrangThai is null)  AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeChotSo
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND qhdct.HinhThucThanhToan NOT IN (6)
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND (bhd.LoaiHoaDon is null or bhd.LoaiHoaDon != 22)
    			GROUP BY qhdct.ID_DoiTuong
    
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeChotSo
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
					AND qhdct.HinhThucThanhToan NOT IN (6)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY qhdct.ID_DoiTuong
    			) AS td
    		    GROUP BY td.ID_DoiTuong
    				UNION ALL
    				-- Công nợ phát sinh trong khoảng thời gian truy vấn
    				SELECT
    			pstv.ID_DoiTuong AS ID_KhachHang,
    			0 AS NoDauKy,
    				SUM(pstv.DoanhThu) + SUM(pstv.TienChi) AS GhiNo,
    				SUM(pstv.TienThu) + SUM(pstv.GiaTriTra) AS GhiCo,
    			    SUM(pstv.CongNo) + SUM(pstv.DoanhThu) + SUM(pstv.TienChi) - SUM(pstv.TienThu) - SUM(pstv.GiaTriTra) AS NoCuoiKy
    			FROM
    			(
    				-- Chốt sổ
    				SELECT 
    				ID_KhachHang As ID_DoiTuong,
    				CongNo AS CongNo,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    				FROM ChotSo_KhachHang
    				where ID_DonVi = @ID_ChiNhanh
    				UNION ALL
    
    			SELECT 
    			bhd.ID_DoiTuong,
    				0 AS CongNo,
    			0 AS GiaTriTra,
    			SUM(bhd.TongThanhToan) AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,25) AND bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeChotSo AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- gia tri trả từ bán hàng
    			UNION All
    			SELECT bhd.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    			SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE (bhd.LoaiHoaDon = '6' OR bhd.LoaiHoaDon = '4') AND bhd.ChoThanhToan = 'false'  AND bhd.NgayLapHoaDon >= @timeChotSo AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY bhd.ID_DoiTuong
    			-- sổ quỹ
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			SUM(qhdct.TienThu) AS TienThu,
    			0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID -- thêm thẻ
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeChotSo AND qhd.NgayLapHoaDon < @timeEnd
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
					AND qhdct.HinhThucThanhToan NOT IN (6)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND (bhd.LoaiHoaDon is null or bhd.LoaiHoaDon != 22)
    			GROUP BY qhdct.ID_DoiTuong
    
    			UNION ALL
    			SELECT 
    			qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS CongNo,
    			0 AS GiaTriTra,
    			0 AS DoanhThu,
    			0 AS TienThu,
    			SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeChotSo AND qhd.NgayLapHoaDon < @timeEnd
    				--AND (qhd.HachToanKinhDoanh is null or qhd.HachToanKinhDoanh = '1')
					AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
					AND qhdct.HinhThucThanhToan NOT IN (6)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    			GROUP BY qhdct.ID_DoiTuong
    			) AS pstv
    		    GROUP BY pstv.ID_DoiTuong
    			)AS HangHoa
    				GROUP BY HangHoa.ID_KhachHang
    				) a
    				LEFT join DM_DoiTuong dt on a.ID_KhachHang = dt.ID
    					left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    				where dt.TheoDoi ='0'
    				and dt.loaidoituong in (select * from splitstring(@loaiKH)) 
    					AND ((select count(Name) from @tblSearch b where     			
    		dt.MaDoiTuong like '%'+b.Name+'%'
    		or dt.TenDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    			or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		)=@count or @count=0)
    				) b
    			LEFT JOin DM_DoiTuong dt on b.ID_KhachHang = dt.ID
    				where b.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) or b.LoaiDoiTuong = 3 or @ID_NhomDoiTuong = ''
    			Group by b.ID_KhachHang, dt.LoaiDoiTuong, b.NoDauKy, b.TongTienChi, b.TongTienThu, b.NoCuoiKy
    			ORDER BY MAX(b.MaKhachHang) DESC
    			END
    		END
    		ELSE
    		BEGIN
    			SELECT 
    		MAX(dt.TenNhomDoiTuongs) as NhomDoiTac,
    		MAX(b.MaKhachHang) as MaDoiTac,
    		MAX(b.TenKhachHang) as TenDoiTac,
    		Case when (b.NoDauKy >= 0) then b.NoDauKy else 0 end as PhaiThuDauKy,
    		Case when (b.NoDauKy < 0) then b.NoDauKy *(-1) else 0 end as PhaiTraDauKy,
    		MAX(b.TongTienChi) as TongTienChi, 
    		MAX(b.TongTienThu) as TongTienThu,
    		Case when (b.NoCuoiKy >= 0) then b.NoCuoiKy else 0 end as PhaiThuCuoiKy,
    		Case when (b.NoCuoiKy < 0) then b.NoCuoiKy *(-1) else 0 end as PhaiTraCuoiKy
    	 FROM
    	(
    	  SELECT 
    			  a.ID_KhachHang, 
    		  dt.MaDoiTuong AS MaKhachHang, 
    		  dt.TenDoiTuong AS TenKhachHang,
    		  a.NoDauKy,
    		 --a.GhiNo As TongTienChi,
    		 -- a.GhiCo As TongTienThu,
    			  iif(a.GhiNo<= 0, iif(a.GhiCo < 0, -a.GhiCo, 0 ), a.GhiNo) as TongTienChi,
    			  iif(a.GhiCo <=0, iif(a.GhiNo < 0, -a.GhiNo, 0 ), a.GhiCo) as TongTienThu,
    		  CAST(ROUND(a.NoDauKy + a.GhiNo - a.GhiCo, 0) as float) as NoCuoiKy,
    		  Case When dtn.ID_NhomDoiTuong is null then
    		  '00000000-0000-0000-0000-000000000000'
    			  else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    			  dt.LoaiDoiTuong
    	  FROM
    	  (
    	  SELECT HangHoa.ID_KhachHang,
    			SUM(HangHoa.NoDauKy) as NoDauKy, 
    			SUM(HangHoa.GhiNo) as GhiNo,
    			SUM(HangHoa.GhiCo) as GhiCo,
    			SUM(HangHoa.NoDauKy + HangHoa.GhiNo - HangHoa.GhiCo) as NoCuoiKy
    		FROM
    		(
    			SELECT
    				td.ID_DoiTuong AS ID_KhachHang,
    				SUM(td.DoanhThu) + SUM(td.TienChi) - SUM(td.TienThu) - SUM(td.GiaTriTra) AS NoDauKy,
    				0 AS GhiNo,
    				0 AS GhiCo,
    				0 AS NoCuoiKy
    			FROM
    			(
    
    				---- CÔNG NỢ ĐẦU KỲ
    				---- doanhthu khachhang
    			SELECT 
    				bhd.ID_DoiTuong,
    				0 AS GiaTriTra,
    				SUM(bhd.PhaiThanhToan) AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,22,25) AND bhd.ChoThanhToan = '0' AND bhd.NgayLapHoaDon < @timeStart
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND bhd.ID_DoiTuong is not null
    			GROUP BY bhd.ID_DoiTuong
					--chi phí hóa đơn
					UNION ALL
						SELECT 
    						cp.ID_NhaCungCap,
    						SUM(cp.ThanhTien) AS GiaTriTra,
    						0 AS DoanhThu,
    						0 AS TienThu,
    						0 AS TienChi
    					FROM BH_HoaDon bhd
						INNER JOIN BH_HoaDon_ChiPhi cp ON cp.ID_HoaDon = bhd.ID
    					WHERE bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon < @timeStart
    					AND bhd.ID_DonVi = @ID_ChiNhanh
    						AND cp.ID_NhaCungCap not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    					GROUP BY cp.ID_NhaCungCap
    				union all
    				---- doanhthu baohiem
    			SELECT 
    				bhd.ID_BaoHiem,
    				0 AS GiaTriTra,
    				SUM(bhd.PhaiThanhToanBaoHiem) AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon in (1,25) AND bhd.ChoThanhToan = '0' AND bhd.NgayLapHoaDon < @timeStart
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				and bhd.ID_BaoHiem is not null
    			GROUP BY bhd.ID_BaoHiem
    
    			-- trahang of khachhag
    			UNION All
    			SELECT bhd.ID_DoiTuong,
    				SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (4,6) AND bhd.ChoThanhToan = '0' AND bhd.NgayLapHoaDon < @timeStart
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND bhd.ID_DoiTuong is not null
    			GROUP BY bhd.ID_DoiTuong
    
    			-- thucthu khachhang + baohiem + ncc
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				SUM(qhdct.TienThu) AS TienThu,
    				0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID 
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai  != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon < @timeStart
				AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND qhdct.ID_DoiTuong is not null
    			
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    
    				-- phieuchi khachhang + ncc+ baohiem
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong AS ID_KhachHang,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon    		
    			WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai  != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon < @timeStart
				AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
    			AND qhd.ID_DonVi = @ID_ChiNhanh
				AND qhdct.HinhThucThanhToan NOT IN (6)
    				AND qhdct.ID_NhanVien is null
					AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND qhdct.ID_DoiTuong is not null
    			GROUP BY qhdct.ID_DoiTuong
    			) AS td
    		    GROUP BY td.ID_DoiTuong
    			UNION ALL
    
    				-- Công nợ phát sinh trong khoảng thời gian truy vấn (---- CÔNG NỢ TRONG KỲ  ------)
    			SELECT
    				pstv.ID_DoiTuong AS ID_KhachHang,
    				0 AS NoDauKy,
    					SUM(pstv.DoanhThu) + SUM(pstv.TienChi) AS GhiCo,
    				SUM(pstv.GiaTriTra) + SUM(pstv.TienThu) AS GhiNo,
    			
    				0 AS NoCuoiKy
    			FROM
    			(
    				-- KhachHang: doanh thu
    			SELECT 
    				bhd.ID_DoiTuong,
    				0 AS GiaTriTra,
    				SUM(bhd.PhaiThanhToan) AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon  in (1,7,19,22,25) AND bhd.ChoThanhToan = '0' AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND bhd.ID_DoiTuong is not null
    			GROUP BY bhd.ID_DoiTuong
				--chi phí hóa đơn
				UNION ALL
					SELECT 
    					cp.ID_NhaCungCap,
    					SUM(cp.ThanhTien) AS GiaTriTra,
    					0 AS DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi
    				FROM BH_HoaDon bhd
					INNER JOIN BH_HoaDon_ChiPhi cp ON cp.ID_HoaDon = bhd.ID
    				WHERE bhd.ChoThanhToan = 'false' AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    				AND bhd.ID_DonVi = @ID_ChiNhanh
    					AND cp.ID_NhaCungCap not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				GROUP BY cp.ID_NhaCungCap
    				union all
    				---- doanhthu baohiem
    			SELECT 
    				bhd.ID_BaoHiem,
    					0 AS GiaTriTra,    				
    				sum( bhd.PhaiThanhToanBaoHiem) AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE bhd.LoaiHoaDon in (1,25) AND bhd.ChoThanhToan = '0'  AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				and bhd.ID_BaoHiem is not null
    			GROUP BY bhd.ID_BaoHiem
    
    			-- khachhang: trahang
    			UNION All
    			SELECT bhd.ID_DoiTuong,
    				SUM(bhd.PhaiThanhToan) AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				0 AS TienChi
    			FROM BH_HoaDon bhd
    			WHERE (bhd.LoaiHoaDon = '6' OR bhd.LoaiHoaDon = '4') AND bhd.ChoThanhToan = 'false'  AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    			AND bhd.ID_DonVi = @ID_ChiNhanh
    				AND bhd.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
    				AND bhd.ID_DoiTuong is not null
    			GROUP BY bhd.ID_DoiTuong
    
    			--  phieuthu: kh + bh + ncc
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				SUM(qhdct.TienThu) AS TienThu,
    				0 AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				Left join BH_HoaDon bhd on qhdct.ID_HoaDonLienQuan = bhd.ID -- thêm thẻ
    			WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai  != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd		
    			AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
				AND qhd.ID_DonVi = @ID_ChiNhanh
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
					AND qhdct.ID_NhanVien is null
    				AND qhdct.ID_DoiTuong is not null
    				--AND (bhd.LoaiHoaDon is null or bhd.LoaiHoaDon != 22)
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    
    					-- phieuchi: kh + bh + ncc
    			UNION ALL
    			SELECT 
    				qhdct.ID_DoiTuong,
    				0 AS GiaTriTra,
    				0 AS DoanhThu,
    				0 AS TienThu,
    				SUM(qhdct.TienThu) AS TienChi
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			WHERE qhd.LoaiHoaDon = '12' 
				AND (qhd.TrangThai  != '0' OR qhd.TrangThai is null) AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd
    			AND (qhd.PhieuDieuChinhCongNo != 3 OR qhd.PhieuDieuChinhCongNo IS NULL)
				AND qhd.ID_DonVi = @ID_ChiNhanh
				AND qhdct.HinhThucThanhToan NOT IN (6)
    				AND qhdct.ID_DoiTuong not in ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000002')
					AND qhdct.ID_NhanVien is null
    				AND qhdct.ID_DoiTuong is not null
					and qhdct.HinhThucThanhToan != 6
    			GROUP BY qhdct.ID_DoiTuong
    			) AS pstv
    		    GROUP BY pstv.ID_DoiTuong
    			)AS HangHoa
    			  	-- LEFT join DM_DoiTuong dt on HangHoa.ID_KhachHang = dt.ID
    				GROUP BY HangHoa.ID_KhachHang
    				) a
    				LEFT join DM_DoiTuong dt on a.ID_KhachHang = dt.ID
    				left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    				where  dt.TheoDoi ='0' 
    				and dt.loaidoituong in (select * from splitstring(@loaiKH)) 
    					AND ((select count(Name) from @tblSearch b where     			
    		dt.MaDoiTuong like '%'+b.Name+'%'
    		or dt.TenDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    			or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		)=@count or @count=0)
    				) b
    			LEFT JOin DM_DoiTuong dt on b.ID_KhachHang = dt.ID
    				where b.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) or b.LoaiDoiTuong = 3 or @ID_NhomDoiTuong = ''
    			Group by b.ID_KhachHang, dt.LoaiDoiTuong, b.NoDauKy, b.TongTienChi, b.TongTienThu, b.NoCuoiKy	
    			ORDER BY MAX(b.MaKhachHang) DESC
    		END
END");

            Sql(@"ALTER PROCEDURE [dbo].[CapNhatThongBaoBaoDuongXe]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert statements for procedure here
    	DECLARE @dateFrom DATETIME, @dateNow DATETIME, @dateFromTemp DATETIME;
    	DECLARE @dateTo DATETIME, @ThoiGianNhacTruoc INT, @LoaiThoiGianNhacTruoc INT, @SoLanLapLai INT, @LoaiThoiGianLapLai INT;
    	SELECT @ThoiGianNhacTruoc = NhacTruocThoiGian, @LoaiThoiGianNhacTruoc = NhacTruocLoaiThoiGian, @SoLanLapLai = SoLanLapLai,
    	@LoaiThoiGianLapLai = LoaiThoiGianLapLai FROM HT_ThongBao_CatDatThoiGian WHERE LoaiThongBao = 4;
    
    	SET @dateNow = dateadd(day, datediff(day, 0, getdate()), 0);
    
    	SET @dateFrom = IIF(@LoaiThoiGianNhacTruoc = 3, DATEADD(DAY, @ThoiGianNhacTruoc, @dateNow), 
    	IIF(@LoaiThoiGianNhacTruoc = 4, DATEADD(MONTH, @ThoiGianNhacTruoc, @dateNow), 
    	IIF(@LoaiThoiGianNhacTruoc = 5, DATEADD(YEAR, @ThoiGianNhacTruoc, @dateNow), @dateNow)));
    	SET @dateTo = dateadd(day, datediff(day, 0, @dateFrom)+1, 0);
    	
    	DECLARE @tblLichBaoDuong TABLE(ID UNIQUEIDENTIFIER, ID_HangHoa UNIQUEIDENTIFIER, ID_HoaDon UNIQUEIDENTIFIER, 
    	LanBaoDuong INT, SoKmBaoDuong INT, NgayBaoDuongDuKien DATETIME, NgayTao DATETIME, TrangThai INT, ID_Xe UNIQUEIDENTIFIER,
    	GhiChu NVARCHAR(MAX), LanNhac INT);
    	--PRINT @dateFrom
    	INSERT INTO @tblLichBaoDuong
    	SELECT * FROM Gara_LichBaoDuong WHERE NgayBaoDuongDuKien BETWEEN @dateFrom AND @dateTo AND TrangThai = 1;
    
    	SET @dateFromTemp = @dateFrom;
    	DECLARE @intFlag INT
    	SET @intFlag = 1
    	WHILE (@intFlag < @SoLanLapLai)
    	BEGIN
    		SET @dateFrom = IIF(@LoaiThoiGianLapLai = 3, DATEADD(DAY, @intFlag*-1, @dateFromTemp), 
    		IIF(@LoaiThoiGianLapLai = 4, DATEADD(MONTH, @intFlag*-1, @dateFromTemp), 
    		IIF(@LoaiThoiGianLapLai = 5, DATEADD(YEAR, @intFlag*-1, @dateFromTemp), @dateFromTemp)));
    		SET @dateTo = dateadd(day, datediff(day, 0, @dateFrom)+1, 0);
    	
    		INSERT INTO @tblLichBaoDuong
    		SELECT * FROM Gara_LichBaoDuong WHERE NgayBaoDuongDuKien BETWEEN @dateFrom AND @dateTo AND TrangThai = 1;
    
    		SET @intFlag = @intFlag + 1;
    	END
    	
    	DECLARE @tblXeBaoDuong TABLE(IDXe UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX));
    
    	INSERT INTO @tblXeBaoDuong
    	SELECT dmx.ID, dmx.BienSo, hh.TenHangHoa FROM @tblLichBaoDuong lbd
    	INNER JOIN Gara_DanhMucXe dmx ON lbd.ID_Xe = dmx.ID
    	INNER JOIN DM_HangHoa hh ON lbd.ID_HangHoa = hh.ID;
    
    	DECLARE @tblNoiDungThongBao TABLE(IDThongBao UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX), TenHangHoaBaoDuong NVARCHAR(MAX), SoLuongHangBaoDuong INT);
    	INSERT INTO @tblNoiDungThongBao
    	SELECT NEWID(),a.BienSo,
    	STUFF((SELECT  N', ' +  TenHangHoa [text()]
    		  FROM @tblXeBaoDuong b WHERE a.IDXe=b.IDXe
    		  for XML PATH (N''),TYPE).
    		  value(N'.',N'NVARCHAR(MAX)'),1,2,N'') AS HangHoaBaoDuong, COUNT(TenHangHoa) AS SoLuongHangBaoDuong
    	FROM @tblXeBaoDuong as a
    	GROUP BY a.IDXe,a.BienSo;
    
    	INSERT INTO HT_ThongBao
    	SELECT IDThongBao, 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE', 4, 
    	'<p onclick=""loaddadoc('''+ convert(nvarchar(50), IDThongBao) +N''')"">Biển số xe <a href="" /#/Gara_LichNhacBaoDuong?s='+ BienSo + '&t=1"">' + BienSo + N'</a> có '+ convert(nvarchar(50), SoLuongHangBaoDuong) + N' hàng hóa: '+ TenHangHoaBaoDuong + N' đến lịch bảo dưỡng.</p>', 
    	GETDATE(), '' FROM @tblNoiDungThongBao;
            END");

            Sql(@"ALTER PROCEDURE [dbo].[CTHD_GetDichVubyDinhLuong]
    @ID_HoaDon [uniqueidentifier],
    @ID_DonViQuiDoi [uniqueidentifier],
    @ID_LoHang [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;	
    
    			select 
    				ctsc.ID_ChiTietGoiDV, --- ~ id of thanhphan
    				ctsc.ID_ChiTietDinhLuong, --- ~ id_quidoi of dichvu
    				ctsc.SoLuong,
    				ctsc.SoLuong - isnull(ctxk.SoLuongXuat,0) as SoLuongConLai,
    				ctsc.ID_DonViQuiDoi, ---- ~ id hanghoa,
    				ctsc.ID_LoHang,
    				hh.QuanLyTheoLoHang,
    				hh.LaHangHoa,
    				hh.DichVuTheoGio,
    				hh.DuocTichDiem,
    				qd.GiaBan,
					qd.GiaNhap,
    				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    				qd.TenDonViTinh,qd.ID_HangHoa,qd.MaHangHoa,
    				ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, 
    				CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
    				hh.LaHangHoa, 
					hh.TenHangHoa, 
					hh.TenHangHoa as TenHangHoaThayThe,
    				hh.ID_NhomHang as ID_NhomHangHoa, ISNULL(hh.GhiChu,'') as GhiChuHH,
    				ctsc.IDChiTietDichVu, ---- ~ id chitietHD of dichvu
					ctsc.ChatLieu,
					isnull(tblVTtri.TenViTris,'') as ViTriKho
    			from 
    			(
    				--- ct hoadon suachua
    				select 
    					cttp.ID as ID_ChiTietGoiDV,
						cttp.ChatLieu,
    					isnull(ctdv.ID_DichVu,cttp.ID_DonViQuiDoi) as ID_ChiTietDinhLuong, -- id_hanghoa/id_dichvu
    					cttp.SoLuong,
    					cttp.ID_DonViQuiDoi,
    					cttp.ID_LoHang,
    					cttp.ID_ChiTietDinhLuong AS IDChiTietDichVu --- used to xuất kho hàng ngoài
    				from BH_HoaDon_ChiTiet cttp
    				left join
    				(
    					select ctm.ID_DonViQuiDoi as ID_DichVu, ctm.ID
    					from BH_HoaDon_ChiTiet ctm where ctm.ID_HoaDon= @ID_HoaDon
    				) ctdv on cttp.ID_ChiTietDinhLuong = ctdv.ID
    				where cttp.ID_DonViQuiDoi = @ID_DonViQuiDoi
    				and ((cttp.ID_LoHang = @ID_LoHang) or (cttp.ID_LoHang is null and @ID_LoHang is null))
    				and cttp.ID_HoaDon= @ID_HoaDon
    			) ctsc
    			left join
    			(
    					---- ct xuatkho 
    				select sum(ct.SoLuong) as SoLuongXuat,
    					ct.ID_ChiTietGoiDV
    				from BH_HoaDon_ChiTiet ct
    				join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    				where hd.ChoThanhToan='0' and hd.LoaiHoaDon=8
    				and hd.ID_HoaDon= @ID_HoaDon
    				and ct.ID_DonViQuiDoi= @ID_DonViQuiDoi 
    				and ((ct.ID_LoHang = @ID_LoHang) or (ct.ID_LoHang is null and @ID_LoHang is null))
    				group by ct.ID_ChiTietGoiDV
    			) ctxk on ctsc.ID_ChiTietGoiDV = ctxk.ID_ChiTietGoiDV
    			join DonViQuiDoi qd on ctsc.ID_ChiTietDinhLuong= qd.ID
    			join DM_HangHoa hh on qd.ID_HangHoa= hh.ID	
    			left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID
				left join
				(
				----- vitrikho -----
					select hh.ID,
							(
    						Select  vt.TenViTri + ',' AS [text()]
    						From dbo.DM_HangHoa_ViTri vt
							join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    						where hh.ID= vth.ID_HangHoa
    						For XML PATH ('')
							) TenViTris
					From DM_HangHoa hh
				) tblVTtri on hh.ID = tblVTtri.ID
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
		select 
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
    	isnull(gv.GiaVon, 0) as GiaVon,
		case tbl.LoaiHangHoa 
			when 1 then 11
			when 2 then 12
			when 3 then 23 end as LoaiHangSearch
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
		or (
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
	) a
	left join
    	(			
    		select ct.ID_DonViQuiDoi, ct.GiaBan as GiaBan2
    		from DM_GiaBan_ChiTiet ct where ct.ID_GiaBan = @ID_BangGia
    	) b on a.ID_DonViQuiDoi= b.ID_DonViQuiDoi
	 where (a.LaHangHoa = 0 or a.TonKho > iif(@Contonkho='1', 0, -99999))
	 and (a.LaHangHoa like @LaHangHoa or a.LoaiHangSearch like @LaHangHoa)
   ) c 	
   ) tbView
   left join DM_HangHoa_Anh anh on tbView.ID= anh.ID_HangHoa and anh.SoThuTu = 1
   )
	select dt.*, 
		isnull(tblVTtri.TenViTris,'') as ViTriKho
	from data_cte dt
	left join 
	(
		select hh.ID,
				(
    			Select  vt.TenViTri + ',' AS [text()]
    			From dbo.DM_HangHoa_ViTri vt
				join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    			where hh.ID= vth.ID_HangHoa							
    			For XML PATH ('')
				) TenViTris
		From DM_HangHoa hh
	) tblVTtri on dt.ID = tblVTtri.ID
	order by dt.NgayHetHan desc
	OFFSET (@CurrentPage* @PageSize) ROWS
	FETCH NEXT @PageSize ROWS ONLY; 

END");

            Sql(@"ALTER PROCEDURE [dbo].[GetCTHDSuaChua_afterXuatKho]
    @ID_HoaDon [nvarchar](max)
AS
BEGIN
    set nocount on
    
    	--- get cthd of hdsc
    	select cthd.*,
    		cthd.SoLuong * isnull(gv.GiaVon,0) as ThanhTien,
    		isnull(gv.GiaVon,0) as GiaVon,
    		isnull(tk.TonKho,0) as TonKho,
    		isnull(nhh.TenNhomHangHoa,'') as TenNhomHangHoa,
    		lo.NgaySanXuat, lo.NgayHetHan, isnull(lo.MaLoHang,'') as MaLoHang, 
    		hh.QuanLyTheoLoHang,
    		hh.LaHangHoa,
    		hh.DichVuTheoGio,
    		hh.DuocTichDiem,
			qd.GiaNhap, ----- used to nhaphang from hoadon/baogia
			isnull(tblVTtri.TenViTris,'') as ViTriKho,
    		qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		qd.TenDonViTinh,qd.ID_HangHoa,qd.MaHangHoa,ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
    		hh.LaHangHoa, hh.TenHangHoa, CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach, hh.ID_NhomHang as ID_NhomHangHoa, ISNULL(hh.GhiChu,'') as GhiChuHH
    	from
    	(
    			select 
    				 ctsc.ID_DonViQuiDoi, ctsc.ID_LoHang,ctsc.ID_HoaDon,
					 max(ctsc.ChatLieu) as ChatLieu,
    				 max(ctsc.ID_DonVi) as ID_DonVi,
    				 sum(SoLuong) as SoLuongMua,
    				 sum(SoLuongXuat) as SoLuongXuat,
    				 sum(SoLuong) - isnull(sum(SoLuongXuat),0) as SoLuong
    			from
    			(
    			select sum(ct.SoLuong) as SoLuong,
    				0 as SoLuongXuat,
					max(ct.ChatLieu) as ChatLieu,
    				ct.ID_DonViQuiDoi,
    				ct.ID_LoHang,
    				ct.ID_HoaDon,
    				hd.ID_DonVi
    			from BH_HoaDon_ChiTiet ct
    			join BH_HoaDon hd on hd.ID= ct.ID_HoaDon
    			where ct.ID_HoaDon= @ID_HoaDon
    			and (ct.ID_ChiTietDinhLuong != ct.ID or ct.ID_ChiTietDinhLuong is null)
				and ct.ID_LichBaoDuong is null ---- khong xuat hang bao duong
    			group by ct.ID_DonViQuiDoi, ct.ID_LoHang,ct.ID_HoaDon,hd.ID_DonVi
    
    			union all
    			-- get cthd daxuat kho
    			select 0 as SoLuong,
    				sum(ct.SoLuong) as SoLuongXuat,
					max(ct.ChatLieu) as ChatLieu,
    				ct.ID_DonViQuiDoi,
    				ct.ID_LoHang,
    				@ID_HoaDon as ID_HoaDon,
    				'00000000-0000-0000-0000-000000000000' as ID_DonVi
    			from BH_HoaDon_ChiTiet ct
    			join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
    			where hd.ID_HoaDon= @ID_HoaDon
    			and hd.ChoThanhToan='0'
				and hd.LoaiHoaDon= 8
    			group by ct.ID_DonViQuiDoi, ct.ID_LoHang
    			)ctsc
    			group by ctsc.ID_DonViQuiDoi, ctsc.ID_LoHang,ctsc.ID_HoaDon
    	) cthd
    	join DonViQuiDoi qd on cthd.ID_DonViQuiDoi= qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
		left join
		(
		----- vitrikho -----
			select hh.ID,
					(
    				Select  vt.TenViTri + ',' AS [text()]
    				From dbo.DM_HangHoa_ViTri vt
					join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    				where hh.ID= vth.ID_HangHoa
    				For XML PATH ('')
					) TenViTris
			From DM_HangHoa hh
		) tblVTtri on hh.ID = tblVTtri.ID
    	left join DM_LoHang lo on cthd.ID_LoHang= lo.ID and hh.ID= lo.ID_HangHoa
    	left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID 
    	left join DM_HangHoa_TonKho tk on (qd.ID = tk.ID_DonViQuyDoi and (lo.ID = tk.ID_LoHang or lo.ID is null) and  tk.ID_DonVi = cthd.ID_DonVi)
    	left join DM_GiaVon gv on (qd.ID = gv.ID_DonViQuiDoi and (lo.ID = gv.ID_LoHang or lo.ID is null) and gv.ID_DonVi = cthd.ID_DonVi) -- lay giavon hientai --> xuatkho gara tu hdsc
    	where hh.LaHangHoa= 1
END");

            Sql(@"ALTER PROCEDURE [dbo].[getList_HangHoaXuatHuybyID]
    @ID_HoaDon [uniqueidentifier],
	@ID_ChiNhanh [uniqueidentifier]
AS
BEGIN
  set nocount on;

		declare @loaiHD int, @ID_HoaDonGoc uniqueidentifier		
		select @loaiHD = LoaiHoaDon, @ID_HoaDonGoc= ID_HoaDon from BH_HoaDon where ID= @ID_HoaDon

		select 
			ctxk.ID,ctxk.ID_DonViQuiDoi,ctxk.ID_LoHang,
			dvqd.ID_HangHoa,
			ctxk.SoLuong, ctxk.SoLuong as SoLuongXuatHuy,
			ctxk.DonGia,
			ctxk.GiaVon, 
			ctxk.GiaTriHuy, ctxk.TienChietKhau as GiamGia,
			ctxk.GhiChu,
			ctxk.SoThuTu,
			ctxk.ChatLieu,
			hd.MaHoaDon,
			hd.NgayLapHoaDon,
			hd.ID_NhanVien,
    		nv.TenNhanVien,
			lh.NgaySanXuat,
    		lh.NgayHetHan,    			
    		dvqd.MaHangHoa,
    		hh.TenHangHoa,
			Case when hh.QuanLyTheoLoHang is null then 'false' else hh.QuanLyTheoLoHang end as QuanLyTheoLoHang, 
    		concat(hh.TenHangHoa , '', dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
    		dvqd.TenDonViTinh as TenDonViTinh,
    		dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		Case when lh.MaLoHang is null then '' else lh.MaLoHang end as TenLoHang,
    		CAST(ROUND(3, 0) as float) as TrangThaiMoPhieu,
			ROUND(ISNULL(TonKho,0),2) as TonKho,
			isnull(tblVTtri.TenViTris,'') as ViTriKho
		from 
		(
		--- get ct if has tpdinhluong
		select max(ct.ID) as ID,
			max(ct.SoThuTu) as SoThuTu,
			ct.ChatLieu,
			ct.ID_DonViQuiDoi,
			ct.ID_LoHang,
			@ID_HoaDon as ID_HoaDon,
			max(ct.GiaVon) as DonGia,
			max(ct.GiaVon) as GiaVon,
			sum(ct.SoLuong) as SoLuong, 
			sum(iif(@loaiHD=8, ct.ThanhTien , ct.SoLuong *ct.GiaVon)) as GiaTriHuy,					
			max(ct.TienChietKhau) as TienChietKhau,
			max(ct.GhiChu) as GhiChu
		from BH_HoaDon_ChiTiet ct
		where ct.ID_HoaDon= @ID_HoaDon and (ct.ChatLieu is null or ct.ChatLieu !='5')
		group by ct.ID_DonViQuiDoi, ct.ID_LoHang, ct.ChatLieu	
		)ctxk
		join BH_HoaDon hd on hd.ID= ctxk.ID_HoaDon
		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
		join DonViQuiDoi dvqd on ctxk.ID_DonViQuiDoi = dvqd.ID
		join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
		left join
		(
		----- vitrikho -----
			select hh.ID,
					(
    				Select  vt.TenViTri + ',' AS [text()]
    				From dbo.DM_HangHoa_ViTri vt
					join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    				where hh.ID= vth.ID_HangHoa
    				For XML PATH ('')
					) TenViTris
			From DM_HangHoa hh
		) tblVTtri on hh.ID = tblVTtri.ID
		left join DM_LoHang lh on ctxk.ID_LoHang = lh.ID
		left join DM_HangHoa_TonKho tk on (dvqd.ID = tk.ID_DonViQuyDoi and (lh.ID = tk.ID_LoHang or lh.ID is null) and  tk.ID_DonVi = @ID_ChiNhanh)
		where (hh.LaHangHoa = 1 and tk.TonKho is not null)
END");

            Sql(@"ALTER PROCEDURE [dbo].[getlist_SuKienToDay_v2]
    @ID_DonVi [uniqueidentifier],
    @Date [datetime]
AS
BEGIN
    set nocount on;
    	DECLARE @SoNgay int = (select top 1  ThoiGianNhacHanSuDungLo from HT_CauHinhPhanMem where ID_DonVi = @ID_DonVi);
    	DECLARE @DateNow datetime= @Date;
    	DECLARE @tblCalendar table (ID uniqueidentifier, ID_DonVi uniqueidentifier, ID_NhanVien uniqueidentifier, NgayGio datetime, PhanLoai int)
    	insert into @tblCalendar exec GetListLichHen_FullCalendar_Dashboard @ID_DonVi,'%%',@Date ;
    	DECLARE @SinhNhat FLOAT, @CongViec FLOAT, @LichHen FLOAT, @SoLoSapHetHan FLOAT, @SoLoHetHan FLOAT, @XeMoiTiepNhan FLOAT, 
		@XeXuatXuong FLOAT, @XeXuatXuong3Ngay FLOAT;;
    
    	select @SinhNhat = Count(ID) from DM_DoiTuong 
    		where TheoDoi != 1 and NgaySinh_NgayTLap is not null
    		and DAY(NgaySinh_NgayTLap) = DAY(@DateNow)
    		and MONTH(NgaySinh_NgayTLap)= MONTH(@DateNow);
    
    		Select @CongViec = COUNT(CASE WHEN PhanLoai = 4 THEN 1 END),
    		@LichHen = COUNT(CASE WHEN PhanLoai = 3 THEN 1 END)
    		from @tblCalendar where PhanLoai IN (3, 4);
    
    		Select 
    		@SoLoSapHetHan = COUNT(CASE WHEN a.SoNgayConHan = @SoNgay and a.SoNgayConHan > 0 THEN 1 END),
    		@SoLoHetHan = COUNT(CASE WHEN a.SoNgayConHan < 1 THEN 1 END)
    		from (
    			SELECT DATEDIFF(day,@DateNow, lh.NgayHetHan) as SoNgayConHan FROM DM_LoHang lh
    			JOIN (Select ID_LoHang, SUM(TonKho) as TonKho from DM_HangHoa_TonKho where ID_DonVi = @ID_DonVi GROUP BY ID_LoHang) tk on lh.ID = tk.ID_LoHang
    			where lh.NgayHetHan is not null 
    			and tk.TonKho > 0
    			) as a;
    
    		SELECT @XeMoiTiepNhan = COUNT(CASE WHEN convert(varchar(10), NgayVaoXuong, 102) 
    		= convert(varchar(10), @DateNow, 102) THEN 1 END),
    			@XeXuatXuong = COUNT(CASE WHEN convert(varchar(10), NgayXuatXuong, 102) 
    		= convert(varchar(10), @DateNow, 102) THEN 1 END),
				@XeXuatXuong3Ngay = COUNT(CASE WHEN convert(varchar(10), NgayXuatXuong, 102) 
    						= convert(varchar(10), DATEADD(day,-2, @DateNow), 102) THEN 1 END)
    		FROM Gara_PhieuTiepNhan
    		WHERE TrangThai != 0
			and ID_DonVi= @ID_DonVi
    
    		DECLARE @KhachHangMoi FLOAT;
    		SELECT @KhachHangMoi = COUNT(ID)
    		FROM DM_DoiTuong
    		WHERE convert(varchar(10), NgayTao, 102) 
    		= convert(varchar(10), @DateNow, 102)
    		and TheoDoi= 0
			and LoaiDoiTuong = 1 and ID not like '%00000000-0000-0000-0000-0000000000%'
    
    	SELECT @SinhNhat AS SinhNhat, @CongViec AS CongViec, @LichHen AS LichHen, @SoLoSapHetHan AS SoLoSapHetHan, @SoLoHetHan AS SoLoHetHan,
    	@XeMoiTiepNhan AS XeMoiTiepNhan, @XeXuatXuong AS XeXuatXuong,@XeXuatXuong3Ngay as  XeXuatXuong3Ngay, @KhachHangMoi AS KhachHangMoi;
END");

            Sql(@"ALTER PROCEDURE [dbo].[getList_XuatHuy]
    @IDChiNhanhs nvarchar(max)= null,
	@DateFrom datetime= null,
	@DateTo datetime= null,
	@LoaiHoaDons nvarchar(max)= null,
	@TrangThais nvarchar(max)= null,
	@TextSearch nvarchar(max)=null,
	@CurrentPage int= null,
	@PageSize int = null
AS
BEGIN
	set nocount on;
	declare @sqlSub nvarchar(max) ='', @whereSub nvarchar(max) ='',@tblSub varchar(max) ='',
	@sql nvarchar(max) ='', @where nvarchar(max) ='', @tblOut varchar(max) ='',
	@paramDefined nvarchar(max) =''

	declare @paramIn nvarchar(max) =' declare @textSeach_isNull int = 1'

	set @whereSub =' where 1 = 1 and hd.loaiHoaDon in (1,8) and (hdct.ID_ChiTietDinhLuong is null or hdct.ID_ChiTietDinhLuong != hdct.ID) and hh.LaHangHoa = 1'
	set @where =' where 1 = 1 '
	set @paramDefined = N'@IDChiNhanhs_In nvarchar(max)= null,
							@DateFrom_In datetime= null,
							@DateTo_In datetime= null,
							@LoaiHoaDons_In nvarchar(max)= null,
							@TrangThais_In nvarchar(max)= null,
							@TextSearch_In nvarchar(max)= null,
							@CurrentPage_In int= null,
							@PageSize_In int = null '


	if isnull(@CurrentPage,'') ='' set @CurrentPage= 0
	if isnull(@PageSize,'') ='' set @PageSize= 10

	if isnull(@IDChiNhanhs,'') !=''
	begin
		set @tblSub = CONCAT(@tblSub ,N'  declare @tblChiNhanh table(ID uniqueidentifier)
										 insert into @tblChiNhanh 
										 select name from dbo.splitstring(@IDChiNhanhs_In); ')
		set @whereSub = CONCAT(@whereSub, N' and exists (select ID from @tblChiNhanh cn where hd.ID_DonVi= cn.ID)' )
	end
	
	if isnull(@DateFrom,'') !=''
		begin
			set @whereSub = CONCAT(@whereSub, N' and hd.NgayLapHoaDon >= @DateFrom_In' )
		end
	if isnull(@DateTo,'') !=''
		begin
			set @whereSub = CONCAT(@whereSub, N' and hd.NgayLapHoaDon < @DateTo_In' )
		end

	if isnull(@LoaiHoaDons,'') !=''
	begin
		set @tblOut = CONCAT(@tblOut ,N'  declare @tblLoai table(Loai int)
									 insert into @tblLoai 
									 select name from dbo.splitstring(@LoaiHoaDons_In) ;')
		set @where = CONCAT(@where, N' and exists (select Loai from @tblLoai loai where tbl.LoaiHoaDon= loai.Loai)' )
	end

	if isnull(@TrangThais,'') !=''
	begin
		set @tblOut = CONCAT(@tblOut ,N'  declare @tblTrangThai table(TrangThai int)
									 insert into @tblTrangThai 
									 select name from dbo.splitstring(@TrangThais_In) ;')
		set @where = CONCAT(@where, N' and exists (select TrangThai from @tblTrangThai tt where tbl.TrangThai= tt.TrangThai)' )
	end

	if isnull(@TextSearch,'') !=''
	begin
		set @paramIn = concat(@paramIn, N' set @textSeach_isNull =0 ')
		set @tblOut = CONCAT(@tblOut ,N'  DECLARE @tblSearchString TABLE (Name [nvarchar](max));
									DECLARE @count int;
									INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch_In, '' '') where Name!='''';
									Select @count =  (Select count(*) from @tblSearchString) ;')
		set @where = CONCAT(@where, N' and ((select count(Name) from @tblSearchString b 
									where tbl.MaHoaDon like ''%'' +b.Name +''%'' 										
										or tbl.NguoiTaoHD like ''%'' +b.Name +''%'' 
										or tbl.DienGiai like N''%''+b.Name+''%''
										or nv.TenNhanVien like ''%''+b.Name+''%''
										or nv.TenNhanVienKhongDau like ''%''+b.Name+''%''
										or nv.TenNhanVienChuCaiDau like ''%''+b.Name+''%''
										or nv.MaNhanVien like ''%''+b.Name+''%''
										or tn.MaPhieuTiepNhan like ''%''+b.Name+''%''
										or hdsc.MaHoaDon like ''%''+b.Name+''%''
										or xe.BienSo like ''%''+b.Name+''%''									
										or tbl.DienGiaiUnsign like N''%''+b.Name+''%''
										)=@count or @count=0)' )
	end

	set @sqlSub = CONCAT(
	N' select 
			hd.ID,
    		hd.ID_NhanVien,
    		hd.ID_DonVi,
			hd.ID_HoaDon,
			hd.ID_PhieuTiepNhan,	
			hd.ID_Xe,
    		hd.MaHoaDon,
    		hd.NgayLapHoaDon,   	
    		ISNULL(hdct.SoLuong * hdct.GiaVon, 0) as TongTienHang,
    		hd.DienGiai,
			hd.DienGiai as DienGiaiUnsign,
			---iif(@textSeach_isNull= 1, hd.DienGiai, dbo.FUNC_ConvertStringToUnsign(hd.DienGiai)) as DienGiaiUnsign,
			hd.ChoThanhToan,
			hd.NguoiTao as NguoiTaoHD, 
			case hd.ChoThanhToan
				when 1 then 1
				when 0 then 2
			else 3 end as TrangThai,
			case hd.ChoThanhToan
				when 1 then N''Tạm lưu''
				when 0 then N''Hoàn thành''
			else N''Hủy bỏ'' end as YeuCau,    
			---- 1.sudung gdv, 2.xuat banle, 3.xuat suachua, 8.xuatkho thuong
			Case 
				when hd.LoaiHoaDon = 8 then case when hd.ID_PhieuTiepNhan is not null then case when hdct.ChatLieu = 4 then 1 else 3 end else 8 end			
				when hd.LoaiHoaDon = 1 and hdct.ChatLieu = 4 then 1 else 2
					 end as LoaiHoaDon,				
			Case when hd.LoaiHoaDon = 8 then case when hd.ID_PhieuTiepNhan is not null 
								then case when hdct.ChatLieu = 4 then N''Phiếu xuất sử dụng gói dịch vụ'' else N''Xuất sửa chữa'' end else N''Phiếu xuất kho'' end
				when hd.LoaiHoaDon = 1 and hdct.ChatLieu = 4 then N''Phiếu xuất sử dụng gói dịch vụ''
				else N''Xuất bán lẻ'' end as LoaiPhieu
			into #hdXK
			from BH_HoaDon hd
			join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
			join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
									', @whereSub)

		set @sql= CONCAT(@paramIn, @tblSub, @tblOut, @sqlSub, '; ',
			N' with data_cte
			as(
				select tbl.ID,
					tbl.MaHoaDon,
					tbl.NgayLapHoaDon,
					tbl.ID_HoaDon,
					tbl.ID_PhieuTiepNhan,
					tbl.ID_Xe,	
					tbl.ID_NhanVien,
					tbl.ID_DonVi,				
					sum(tbl.TongTienHang) as TongTienHang,
					tbl.ChoThanhToan,
					tbl.LoaiHoaDon,
					tbl.LoaiPhieu,
					tbl.NguoiTaoHD,
					tbl.YeuCau,
					tbl.DienGiai,
					tn.MaPhieuTiepNhan,
					hdsc.MaHoaDon as MaHoaDonGoc,
					xe.BienSo,					
					dv.TenDonVi,
					nv.TenNhanVien,
					dt.MaDoiTuong,
					dt.TenDoiTuong
			from #hdXK tbl 
			join DM_DonVi dv on tbl.ID_DonVi = dv.ID
			left join BH_HoaDon hdsc on tbl.ID_HoaDon= hdsc.ID
			left join Gara_PhieuTiepNhan tn on tbl.ID_PhieuTiepNhan= tn.ID
			left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
    		left join NS_NhanVien nv on tbl.ID_NhanVien = nv.ID
			left join DM_DoiTuong dt on tn.ID_KhachHang = dt.ID
			', @where ,
			N' group by 
					tbl.ID,
					tbl.MaHoaDon,
					tbl.NgayLapHoaDon,
					tbl.ID_HoaDon,
					tbl.ID_PhieuTiepNhan,
					tbl.ID_Xe,
					tbl.ID_NhanVien,
					tbl.ID_DonVi,		
					tbl.ChoThanhToan,
					tbl.LoaiHoaDon,
					tbl.LoaiPhieu,
					tbl.NguoiTaoHD,
					tbl.YeuCau,
					tbl.DienGiai,
					tn.MaPhieuTiepNhan,
					hdsc.MaHoaDon,
					xe.BienSo,					
					dv.TenDonVi,
					nv.TenNhanVien,
					dt.MaDoiTuong,
					dt.TenDoiTuong
			),
			count_cte
			as (
			select count(ID) as TotalRow,
				sum(TongTienHang) as SumTongTienHang
			from data_cte
			)
			select dt.*, cte.*
			from data_cte dt
			cross join count_cte cte
			order by dt.NgayLapHoaDon desc
			OFFSET (@CurrentPage_In* @PageSize_In) ROWS
			FETCH NEXT @PageSize_In ROWS ONLY; 
			')

		
			
			exec sp_executesql @sql, @paramDefined,
					 @IDChiNhanhs_In = @IDChiNhanhs,
					@DateFrom_In = @DateFrom,
					@DateTo_In = @DateTo,
					@LoaiHoaDons_In = @LoaiHoaDons,
					@TrangThais_In = @TrangThais,
					@TextSearch_In =@TextSearch,
					@CurrentPage_In = @CurrentPage,
					@PageSize_In = @PageSize
				
	
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetListComBo_ofCTHD]
    @ID_HoaDon [uniqueidentifier],
    @IDChiTiet [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	declare @ID_DonVi uniqueidentifier = (select top 1 ID_DonVi from BH_HoaDon where ID= @ID_HoaDon)
    
    	select ctsd.ID_ChiTietGoiDV, sum(SoLuong) as SoLuongSuDung
    	into #tblSDDV 
    	from BH_HoaDon_ChiTiet ctsd
    	where exists (select ID from BH_HoaDon_ChiTiet ct where ct.ID_HoaDon= @ID_HoaDon and ct.ID_ChiTietGoiDV =  ctsd.ID_ChiTietGoiDV)
    	group by ctsd.ID_ChiTietGoiDV
    
    	select tbl.*, 
    		tbl.SoLuong as SoLuongMua,		
    		isnull(ctt.SoLuongTra,0) as SoLuongTra,
    		isnull(ctt.SoLuongDung,0) as SoLuongDVDaSuDung,
    		tbl.SoLuong -isnull(ctt.SoLuongTra,0) - isnull(ctt.SoLuongDung,0) as SoLuongDVConLai,--- use when print
    		tbl.SoLuong -isnull(ctt.SoLuongTra,0) - isnull(ctt.SoLuongDung,0) as SoLuongConLai -- use when trahang
    		
    		FROM 
    		(
    			SELECT
    				ct.ID,ct.ID_HoaDon,DonGia,ct.GiaVon,SoLuong,ThanhTien,ThanhToan,ct.ID_DonViQuiDoi, ct.ID_ChiTietDinhLuong, ct.ID_ChiTietGoiDV,
    				ct.TienChietKhau AS GiamGia,PTChietKhau,ct.GhiChu,ct.TienChietKhau,
    				(ct.DonGia - ct.TienChietKhau) as GiaBan,
					qd.GiaBan as GiaBanHH, --- used to nhaphang from hoadon
    				CAST(SoThuTu AS float) AS SoThuTu,ct.ID_KhuyenMai, ISNULL(ct.TangKem,'0') as TangKem, ct.ID_TangKem,
    					-- replace char enter --> char space
    				(REPLACE(REPLACE(TenHangHoa,CHAR(13),''),CHAR(10),'') +
    				CASE WHEN (qd.ThuocTinhGiaTri is null or qd.ThuocTinhGiaTri = '') then '' else '_' + qd.ThuocTinhGiaTri end +
    				CASE WHEN TenDonVitinh = '' or TenDonViTinh is null then '' else ' (' + TenDonViTinh + ')' end +
    				CASE WHEN MaLoHang is null then '' else '. Lô: ' + MaLoHang end) as TenHangHoaFull,
    				
    				hh.ID AS ID_HangHoa,
					hh.LaHangHoa,
					hh.QuanLyTheoLoHang,
				
					iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='', hh.TenHangHoa,ct.TenHangHoaThayThe) as TenHangHoa,
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
    					CAST(ISNULL(ct.PTThue,0) as float) as PTThue,
    					CAST(ISNULL(ct.TienThue,0) as float) as TienThue,
    					CAST(ISNULL(ct.ThoiGianBaoHanh,0) as float) as ThoiGianBaoHanh,
    					CAST(ISNULL(ct.LoaiThoiGianBH,0) as float) as LoaiThoiGianBH,
    					Case when hh.LaHangHoa='1' then 0 else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end PhiDichVu,
    					Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end LaPTPhiDichVu,
    					CAST(0 as float) as TongPhiDichVu, -- set default PhiDichVu = 0 (caculator again .js)
    					CAST(ISNULL(ct.Bep_SoLuongYeuCau,0) as float) as Bep_SoLuongYeuCau,
    					CAST(ISNULL(ct.Bep_SoLuongHoanThanh,0) as float) as Bep_SoLuongHoanThanh, -- view in CTHD NhaHang
    					CAST(ISNULL(ct.Bep_SoLuongChoCungUng,0) as float) as Bep_SoLuongChoCungUng,
    					ISNULL(hh.SoPhutThucHien,0) as SoPhutThucHien, -- lay so phut theo cai dat
    					ISNULL(ct.ThoiGianThucHien,0)  as ThoiGianThucHien,-- sophut thuc te thuchien	
    					ISNULL(ct.QuaThoiGian,0)  as QuaThoiGian,
    				
    					case when hh.LaHangHoa='0' then 0 else ISNULL(tk.TonKho,0) end as TonKho,
    					ct.ID_ViTri,
    					ISNULL(vt.TenViTri,'') as TenViTri,			
    					ct.ThoiGian,
						ct.ThoiGianHoanThanh, ISNULL(hh.GhiChu,'') as GhiChuHH,
    					ISNULL(ct.DiemKhuyenMai,0) as DiemKhuyenMai,
    					ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
    					ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
						ISNULL(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
    					ct.ChatLieu,
    					isnull(ct.DonGiaBaoHiem,0) as DonGiaBaoHiem,
    					isnull(ct.TenHangHoaThayThe,hh.TenHangHoa) as TenHangHoaThayThe,
    					ct.ID_LichBaoDuong,
    					iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
    					ct.ID_ParentCombo
    					
    		FROM BH_HoaDon hd
    		JOIN BH_HoaDon_ChiTiet ct ON hd.ID= ct.ID_HoaDon
    		JOIN DonViQuiDoi qd ON ct.ID_DonViQuiDoi = qd.ID
    		JOIN DM_HangHoa hh ON qd.ID_HangHoa= hh.ID    		
    		left JOIN DM_NhomHangHoa nhh ON hh.ID_NhomHang= nhh.ID    							
    		LEFT JOIN DM_LoHang lo ON ct.ID_LoHang = lo.ID
    		left join DM_HangHoa_TonKho tk on ct.ID_DonViQuiDoi= tk.ID_DonViQuyDoi and tk.ID_DonVi= @ID_DonVi
			and (ct.ID_LoHang= tk.ID_LoHang OR (ct.ID_LoHang is null and tk.ID_LoHang is null))
    		left join DM_ViTri vt on ct.ID_ViTri= vt.ID
    		-- chi get CT khong phai la TP dinh luong
    		WHERE ct.ID_HoaDon = @ID_HoaDon
    				and ct.ID_ParentCombo like @IDChiTiet
    					and ct.ID_ParentCombo is not null
    					and ct.ID_ParentCombo != ct.ID
    					
    			) tbl
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
    	) ctt on tbl.ID = ctt.ID_ChiTietGoiDV
END");

            Sql(@"ALTER PROCEDURE [dbo].[getListDanhSachHHImport]
    @MaLoHangIP [nvarchar](max),
    @MaHangHoaIP [nvarchar](max),
    @ID_DonViIP [uniqueidentifier],
    @TimeIP [datetime]
AS
BEGIN
    select hh.*,
		isnull(tblVTtri.TenViTris,'') as ViTriKho
	from
	(
    select 
    	dvqd.ID as ID_DonViQuiDoi,
    	hh.ID as ID,
    	lh.ID as ID_LoHang,
    	case when hh.QuanLyTheoLoHang is null then 'false' else hh.QuanLyTheoLoHang end as QuanLyTheoLoHang,
    	dvqd.MaHangHoa,
    	hh.TenHangHoa,
    	dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    	dvqd.TenDonViTinh,
    		dvqd.TyLeChuyenDoi,
    		dvqd.GiaNhap,
			dvqd.GiaBan,
    	Case when lh.ID is null then '' else lh.MaLoHang end as MaLoHang,
    	Case when gv.ID is null then 0 else Cast(round(gv.GiaVon, 0) as float) end as GiaVon,
    		hhtonkho.TonKho as TonKho,
    		Case when lh.ID is null then'' else lh.NgayHetHan end as NgayHetHan,
			Case when lh.ID is null then'' else lh.NgaySanXuat end as NgaySanXuat
    	FROM 

    	DonViQuiDoi dvqd 
    	inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		left join DM_LoHang lh on lh.ID_HangHoa = hh.ID and lh.MaLoHang = @MaLoHangIP 
    		left join DM_GiaVon gv on (dvqd.ID = gv.ID_DonViQuiDoi and (lh.ID = gv.ID_LoHang or gv.ID_LoHang is null) and gv.ID_DonVi = @ID_DonViIP)
			left join DM_HangHoa_TonKho hhtonkho on dvqd.ID = hhtonkho.ID_DonViQuyDoi and (hhtonkho.ID_LoHang = gv.ID_LoHang or gv.ID_LoHang is null) and hhtonkho.ID_DonVi = @ID_DonViIP
    	where dvqd.MaHangHoa = @MaHangHoaIP 
    		and dvqd.Xoa = 0
    		and hh.TheoDoi = 1 
		)hh
		left join
		(
		----- vitrikho -----
			select hh.ID,
					(
    				Select  vt.TenViTri + ',' AS [text()]
    				From dbo.DM_HangHoa_ViTri vt
					join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    				where hh.ID= vth.ID_HangHoa
    				For XML PATH ('')
					) TenViTris
			From DM_HangHoa hh
		) tblVTtri on hh.ID = tblVTtri.ID
    	order by NgayHetHan
   
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetListHHSearch]
    @ID_ChiNhanh [nvarchar](max),
    @Search [nvarchar](max),
    @SearchCoDau [nvarchar](max),
	@ConTonKho int
AS
BEGIN
    	DECLARE @tablename TABLE(Name [nvarchar](max))
    	DECLARE @tablenameChar TABLE(Name [nvarchar](max))
    	DECLARE @count int
    	DECLARE @countChar int

    	INSERT INTO @tablename(Name) select  Name from [dbo].[splitstring](@Search+',') where Name!='';   
    	Select @count =  (Select count(*) from @tablename);

		INSERT INTO @tablenameChar(Name) select  Name from [dbo].[splitstring](@SearchCoDau+',') where Name!='';
		Select @countChar =   (Select count(*) from @tablenameChar);


		select 
			ID_DonViQuiDoi,
			ID,
			MaHangHoa,
			TenHangHoa,
			ThuocTinh_GiaTri,
			TenDonViTinh,
			QuanLyTheoLoHang,
			TyLeChuyenDoi,
			LaHangHoa,
			GiaBan,
			GiaNhap,
			b.TonKho,
			SrcImage,
			ID_LoHang,
			MaLoHang,
			NgaySanXuat,
			NgayHetHan,
			QuyCach,
			iif(b.LaHangHoa ='1', b.GiaVon, dbo.GetGiaVonOfDichVu(@ID_ChiNhanh,b.ID_DonViQuiDoi)) as GiaVon,
			iif(b.LoaiHangHoa is null, iif(b.LaHangHoa='1',1,2),b.LoaiHangHoa) as LoaiHangHoa
		from
		(
		select tbl.*,			
			Case When gv.ID is null then 0 else CAST(ROUND(( gv.GiaVon), 0) as float) end as GiaVon,
			ISNULL(hhtonkho.TonKho, 0) as TonKho,
			ISNULL(an.URLAnh,'/Content/images/iconbepp18.9/gg-37.png') as SrcImage
		from
		(
		select top 500
			dvqd1.ID as ID_DonViQuiDoi,
    		dhh1.ID,
			dvqd1.ID_HangHoa,
			dvqd1.MaHangHoa,
    		dhh1.TenHangHoa,
			dvqd1.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		dvqd1.TenDonViTinh,
			dhh1.QuanLyTheoLoHang,
			dvqd1.TyLeChuyenDoi,
			dhh1.LaHangHoa,
			dhh1.LoaiHangHoa,					
			dvqd1.GiaBan,
			dvqd1.GiaNhap,			
			lh1.MaLoHang,
    		lh1.NgaySanXuat,
			lh1.NgayHetHan,
			Case when lh1.ID is null then null else lh1.ID end as ID_LoHang,
			iif(dhh1.QuyCach is null or dhh1.QuyCach=0, 1, dhh1.QuyCach) as QuyCach
		from DonViQuiDoi dvqd1
		join DM_HangHoa dhh1 on dvqd1.ID_HangHoa = dhh1.ID
		left join DM_LoHang lh1 on dvqd1.ID_HangHoa = lh1.ID_HangHoa and (lh1.TrangThai = 1 or lh1.TrangThai is null)
		where dvqd1.Xoa = 0 
		and 
			((select count(*) from @tablename b where 
    			dvqd1.MaHangHoa like '%'+b.Name+'%' 
				or dhh1.TenHangHoa like '%'+b.Name+'%' 
    			or dhh1.TenHangHoa_KhongDau like '%'+b.Name+'%' 	
				or lh1.MaLoHang  like '%'+b.Name+'%' 	
				)=@count or @count=0)
				
			and	(
				(select count(*) from @tablenameChar b where 
    				dvqd1.MaHangHoa like '%'+b.Name+'%' 
					or dhh1.TenHangHoa like '%'+b.Name+'%' 
    				or dhh1.TenHangHoa_KhongDau like '%'+b.Name+'%' 	
					or lh1.MaLoHang  like '%'+b.Name+'%' 	
					)=@countChar or @countChar=0)								
				
    )tbl
	LEFT join DM_HangHoa_Anh an on (tbl.ID_HangHoa = an.ID_HangHoa and (an.sothutu = 1 or an.ID is null))
	left join DM_GiaVon gv on tbl.ID_DonViQuiDoi = gv.ID_DonViQuiDoi 
		and (tbl.ID_LoHang = gv.ID_LoHang or tbl.ID_LoHang is null) and gv.ID_DonVi = @ID_ChiNhanh
	left join DM_HangHoa_TonKho hhtonkho on tbl.ID_DonViQuiDoi = hhtonkho.ID_DonViQuyDoi 
	and (hhtonkho.ID_LoHang = tbl.ID_LoHang or tbl.ID_LoHang is null) 
		and hhtonkho.ID_DonVi = @ID_ChiNhanh
	) b
	 where b.LaHangHoa = 0 or b.TonKho > iif(@ConTonKho=1, 0, -99999)
	order by NgayHetHan	
END");

            Sql(@"ALTER PROCEDURE [dbo].[getListXuatKho_Import]
    @MaHangHoa [nvarchar](max),
    @MaLohang [nvarchar](max),
    @SoLuong [float],
    @ID_ChiNhanh [uniqueidentifier]
AS
BEGIN

	select hh.*,
		isnull(tblVTtri.TenViTris,'') as ViTriKho
	from
	(
    SELECT 
			dvqd3.ID_HangHoa as ID,
    		dvqd3.ID as ID_DonViQuiDoi, 			
    		Case When a.ID_LoHang is null then NEWID() else a.ID_LoHang end as ID_LoHang,
    		dvqd3.MaHangHoa,
    		a.TenHangHoa,
    		dvqd3.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		dvqd3.TenDonViTinh, 
    		a.QuanLyTheoLoHang,			
    		Case when gv.ID is null then 0 else CAST(ROUND((gv.GiaVon), 0) as float) end  as GiaVon, 
    		CAST(ROUND((dvqd3.GiaBan), 0) as float) as GiaBan,  
    		CAST(ROUND((@SoLuong), 3) as float) as SoLuong,
    		CAST(ROUND((@SoLuong), 3) as float) as SoLuongXuatHuy,
    		Case when gv.ID is null then 0 else CAST(ROUND((@SoLuong * gv.GiaVon), 0) as float) end  as GiaTriHuy,  
    		CAST(ROUND(a.TonCuoiKy, 3) as float) as TonKho,
    		a.MaLoHang as TenLoHang,
    		a.NgaySanXuat,
    		a.NgayHetHan
    	FROM 
    		(
    		SELECT 
    		dvqd.ID as ID_DonViQuiDoi,
    		dhh.TenHangHoa As TenHangHoa,
    		dvqd.TenDonViTinh AS TenDonViTinh,
    		dhh.QuanLyTheoLoHang,
    		lh.ID As ID_LoHang,
    		Case when @MaLohang != '' then (Case when lh.MaLohang is null or dhh.QuanLyTheoLoHang = '0' then '' else lh.MaLoHang end) else '' end As MaLoHang,
    		Case when @MaLohang != '' then (lh.NgaySanXuat) else null end As NgaySanXuat,
    		Case when @MaLohang != '' then (lh.NgayHetHan) else null end As NgayHetHan,
    		lh.TrangThai,
    		ISNULL(HangHoa.TonCuoiKy,0) AS TonCuoiKy
    		FROM 
    		DonViQuiDoi dvqd 
    		left join
    		(
				Select ID_DonViQuyDoi as ID_DonViQuiDoi,
				Case when ID_LoHang is null then '10000000-0000-0000-0000-000000000001' else ID_LoHang end as ID_LoHang,
				TonKho as TonCuoiKy
				FROM DM_HangHoa_TonKho tk
				where tk.ID_DonVi = @ID_ChiNhanh
    		) AS HangHoa
    		on dvqd.ID = HangHoa.ID_DonViQuiDoi
    		INNER JOIN DM_HangHoa dhh ON dhh.ID = dvqd.ID_HangHoa
    		LEFT JOIN DM_NhomHangHoa dnhh ON dnhh.ID = dhh.ID_NhomHang
    		LEFT JOIN DM_LoHang lh on HangHoa.ID_LoHang = lh.ID
    		Where dvqd.Xoa = '0' and dhh.TheoDoi = 1
    		and lh.TrangThai = 1 or lh.TrangThai is null
		) a
    	LEFT Join DonViQuiDoi dvqd3 on a.ID_DonViQuiDoi = dvqd3.ID
    	LEFT Join DM_GiaVon gv on dvqd3.ID = gv.ID_DonViQuiDoi and (gv.ID_LoHang = a.ID_LoHang or a.ID_LoHang is null) and gv.ID_DonVi = @ID_ChiNhanh
    	Where dvqd3.MaHangHoa = @MaHangHoa
    	and a.MaLoHang = @MaLoHang
   ) hh
   left join
	(
	----- vitrikho -----
		select hh.ID,
				(
    			Select  vt.TenViTri + ',' AS [text()]
    			From dbo.DM_HangHoa_ViTri vt
				join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    			where hh.ID= vth.ID_HangHoa
    			For XML PATH ('')
				) TenViTris
		From DM_HangHoa hh
	) tblVTtri on hh.ID = tblVTtri.ID
   order by hh.NgayHetHan
END");

            Sql(@"ALTER PROCEDURE [dbo].[JqAuto_HoaDonSC]
    @IDChiNhanhs [nvarchar](max),
	@ID_PhieuTiepNhan [nvarchar](max),
    @TextSearch [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;

	-- get list HSSC by chinhah
		select  hd.ID, hd.MaHoaDon, hd.ID_PhieuTiepNhan, hd.NgayLapHoaDon, hd.ID_Xe
		into #temp
    	from BH_HoaDon hd
		where hd.LoaiHoaDon= 25
		and exists (select Name from dbo.splitstring(@IDChiNhanhs) dv where dv.Name= hd.ID_DonVi )
    	and hd.ID_PhieuTiepNhan is not null
    	and hd.ChoThanhToan= 0
		and hd.ID_PhieuTiepNhan like @ID_PhieuTiepNhan

	select hdsc.*, tn.MaPhieuTiepNhan, xe.BienSo
	from #temp hdsc	
	join 
	(
			select tbl2.id, tbl2.SoLuongConLai
			from(
				select tbl.ID,
					sum(tbl.SoLuongBan) - sum(isnull(tbl.SoLuongTra,0)) as SoLuongConLai
				from
				(
					-- sum (slMua) from hdsc
					Select 
    					hd.ID,
    					Sum(ISNULL(hdct.SoLuong, 0)) as SoLuongBan,
						0 as SoLuongTra
    				from #temp hd    			
    				inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon   
					join DonViQuiDoi qd on hdct.ID_DonViQuiDoi = qd.ID 
					join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
					--where hh.LaHangHoa='1' -- chi get hanghoa
    				Group by hd.ID

					-- sum (slXuatKho) from hdxk
					union all
					select 
    					hdsc.ID,
    					0 as SoLuongBan,						
    					Sum(ISNULL(hdct.SoLuong, 0)) as SoLuongTra
    				from BH_HoaDon hdxk
					join #temp hdsc on hdxk.ID_HoaDon = hdsc.ID
    				join BH_HoaDon_ChiTiet hdct on hdxk.ID = hdct.ID_HoaDon
    				where hdct.ID_ChiTietGoiDV is not null
    				and hdxk.ChoThanhToan='0'    			
    				Group by hdsc.ID
				)tbl group by tbl.ID
				)tbl2
	) hdsCL on hdsc.ID = hdsCL.ID
	join Gara_PhieuTiepNhan tn on hdsc.ID_PhieuTiepNhan = tn.ID
   join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
   where ---- do 1so phieutiepnhan da xuatxuong (TrangThai = 3) nhung khong luu ngayxuatxuong
   tn.TrangThai in (1,2)
    	and (tn.MaPhieuTiepNhan like @TextSearch 
    		or xe.BienSo like @TextSearch 		
    		or hdsc.MaHoaDon like @TextSearch 		
    		)	
	order by tn.NgayVaoXuong desc , hdsc.NgayLapHoaDon desc
END");

            Sql(@"ALTER PROCEDURE [dbo].[JqAuto_PhieuTiepNhan]
    @IDChiNhanhs [nvarchar](max),
    @TextSearch [nvarchar](max),
    @CustomerID [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	select top 10 tn.ID, tn.MaPhieuTiepNhan, xe.BienSo, tn.ID_KhachHang,		
		tn.ID_BaoHiem, 
		tn.ID_Xe,
		bh.TenDoiTuong as TenBaoHiem,
		tn.SoDienThoaiLienHeBH, tn.NguoiLienHeBH
    	from Gara_PhieuTiepNhan tn
    	join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID	
		left join DM_DoiTuong bh on tn.ID_BaoHiem= bh.ID
		left join DM_DoiTuong kh on tn.ID_KhachHang= kh.ID
    	where tn.ID_KhachHang like @CustomerID	
    	and tn.TrangThai in (1,2)
    	and exists (select Name from dbo.splitstring(@IDChiNhanhs) dv where dv.Name= tn.ID_DonVi )
    	and (tn.MaPhieuTiepNhan like @TextSearch 
    		or xe.BienSo like @TextSearch 		
    		)
    order by tn.NgayVaoXuong desc
END");

            Sql(@"drop index if exists IX_ID_HoaDon on dbo.BH_HoaDon
CREATE NONCLUSTERED INDEX IX_ID_HoaDon   
    ON dbo.BH_HoaDon ([ID_HoaDon])	
GO  ");

        }
        
        public override void Down()
        {
			DropStoredProcedure("[dbo].[CaiDatTienDo_GetKhoangThoiGian]");
			DropStoredProcedure("[dbo].[Gara_CheckTienDoCongViec]");
			DropStoredProcedure("[dbo].[GetHoaDonBaoGia_ofXeDangSua]");
			DropStoredProcedure("[dbo].[GetLichNhacBaoDuong_TheoXe]");
			DropStoredProcedure("[dbo].[GetListThongBao_ChuaDoc]");
			DropStoredProcedure("[dbo].[Insert_ThongBaoBaoDuong_TheoXe]");
			DropStoredProcedure("[dbo].[UpdateThongBao_CongViecDaXuLy]");
        }
    }
}
